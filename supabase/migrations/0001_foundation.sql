-- DocumentaryForge 0001: foundation (documentaries + durable job engine + external-operation ledger)
-- STATUS (Phase 1.1): UNTESTED AGAINST POSTGRESQL. This file mirrors src/forge/jobs.py, whose
-- behaviour is tested locally on SQLite. No PostgreSQL was available when this was written, so
-- it has never been executed. Do not claim PostgreSQL compatibility until Phase 2 has run this
-- file in a real Supabase project and repeated the same scenarios against these functions.
-- Because it has never been applied anywhere, 0001 was revised in place. Once applied to any real
-- project, every further change MUST be a new numbered migration file.
--
-- Access model: RLS is enabled on every table with NO policies, so anon/authenticated roles get
-- nothing. Workers and server-side API routes use the service-role key (bypasses RLS). All
-- functions and the view are executable/selectable by service_role only.
-- Custom SQLSTATEs: DF001 idempotency_conflict, DF002 dependency_cycle, DF003 dependency_error,
-- DF004 operation_conflict. Lost leases are reported as false/null/zero rows (client raises LeaseLost).

-- ---------------------------------------------------------------- tables
create table if not exists public.documentaries (
    id          text primary key,                 -- e.g. 'indus_valley_001'
    title       text not null default '',
    brief       jsonb not null default '{}'::jsonb,
    status      text not null default 'draft',
    created_at  timestamptz not null default now()
);

create table if not exists public.pipeline_jobs (
    seq                  bigint generated always as identity,   -- deterministic FIFO tiebreak
    id                   uuid primary key default gen_random_uuid(),
    documentary_id       text not null references public.documentaries(id) on delete cascade,
    stage                text not null,
    required_capability  text not null default 'cpu',
    payload              jsonb not null default '{}'::jsonb,
    idempotency_key      text not null,
    input_hash           text not null,                          -- sha256 of canonical {payload, capability}
    priority             int  not null default 100,
    status               text not null default 'queued'
        check (status in ('queued','claimed','running','checkpointed','succeeded',
                          'retryable_failed','permanently_failed','dead_letter',
                          'needs_review','cancelled')),
    attempts             int  not null default 0,
    max_attempts         int  not null default 3 check (max_attempts >= 1),
    run_after            timestamptz not null default now(),
    lease_owner          text,
    lease_expires_at     timestamptz,
    last_heartbeat_at    timestamptz,
    last_error           text,
    blocked_by           uuid references public.pipeline_jobs(id) on delete set null,
    result               jsonb,
    created_at           timestamptz not null default now(),
    updated_at           timestamptz not null default now(),
    -- Identity is scoped: the same key in another documentary or stage is a different job.
    unique (documentary_id, stage, idempotency_key)
);
create index if not exists pipeline_jobs_claim_idx
    on public.pipeline_jobs (required_capability, status, priority, seq);
create index if not exists pipeline_jobs_documentary_idx on public.pipeline_jobs (documentary_id);

create table if not exists public.job_dependencies (
    job_id            uuid not null references public.pipeline_jobs(id) on delete cascade,
    depends_on_job_id uuid not null references public.pipeline_jobs(id) on delete cascade,
    primary key (job_id, depends_on_job_id),
    check (job_id <> depends_on_job_id)
);

create table if not exists public.job_attempts (
    job_id     uuid not null references public.pipeline_jobs(id) on delete cascade,
    attempt    int  not null,
    worker_id  text not null,
    started_at timestamptz not null default now(),
    ended_at   timestamptz,
    outcome    text,
    error      text,
    primary key (job_id, attempt)
);

create table if not exists public.job_checkpoints (
    job_id     uuid not null references public.pipeline_jobs(id) on delete cascade,
    step       text not null,
    data       jsonb not null,
    attempt    int  not null,
    created_at timestamptz not null default now(),
    primary key (job_id, step)
);

-- Durable ledger for external side effects (uploads, downloads, TTS calls, exports).
create table if not exists public.external_operations (
    operation_id   text primary key,              -- deterministic: see src/forge/idempotency.py
    documentary_id text not null references public.documentaries(id) on delete cascade,
    stage          text not null,
    op_name        text not null,
    input_hash     text not null,
    status         text not null default 'started' check (status in ('started','committed')),
    output         jsonb,
    output_hash    text,
    executions     int  not null default 1,
    job_id         uuid references public.pipeline_jobs(id) on delete set null,
    last_worker    text,
    last_attempt   int,
    created_at     timestamptz not null default now(),
    updated_at     timestamptz not null default now(),
    committed_at   timestamptz
);

create table if not exists public.workers (
    worker_id    text primary key,
    worker_type  text not null,                  -- 'github_actions' | 'kaggle' | 'local'
    capabilities text[] not null default '{}',
    last_seen_at timestamptz not null default now()
);

create table if not exists public.audit_events (
    id             bigserial primary key,
    documentary_id text,
    job_id         uuid,
    event          text not null,
    detail         jsonb not null default '{}'::jsonb,
    created_at     timestamptz not null default now()
);

alter table public.documentaries        enable row level security;
alter table public.pipeline_jobs        enable row level security;
alter table public.job_dependencies     enable row level security;
alter table public.job_attempts         enable row level security;
alter table public.job_checkpoints      enable row level security;
alter table public.external_operations  enable row level security;
alter table public.workers              enable row level security;
alter table public.audit_events         enable row level security;

-- ---------------------------------------------------------------- dependency guard
-- Rejects self-edges, cross-documentary edges, edges on jobs that already started, and cycles.
-- Fires on every insert, so enqueue_job and add_dependency are both protected.
-- The advisory lock serialises dependency inserts: without it two concurrent transactions could
-- each add one half of a cycle (each would only see committed rows).
create or replace function public.job_dependencies_guard() returns trigger
language plpgsql as $$
declare jdoc text; ddoc text; jst text; cyc boolean;
begin
    if new.job_id = new.depends_on_job_id then
        raise exception 'dependency_cycle: a job cannot depend on itself' using errcode = 'DF002';
    end if;
    perform pg_advisory_xact_lock(hashtext('documentaryforge.job_dependencies'));
    select documentary_id, status into jdoc, jst from public.pipeline_jobs where id = new.job_id;
    select documentary_id into ddoc from public.pipeline_jobs where id = new.depends_on_job_id;
    if jdoc is null or ddoc is null then
        raise exception 'dependency_error: unknown job' using errcode = 'DF003';
    end if;
    if jdoc <> ddoc then
        raise exception 'dependency_error: cross-documentary dependency' using errcode = 'DF003';
    end if;
    if jst not in ('queued','retryable_failed','needs_review') then
        raise exception 'dependency_error: job already started' using errcode = 'DF003';
    end if;
    with recursive reach(id) as (
        select new.depends_on_job_id
        union
        select d.depends_on_job_id from public.job_dependencies d join reach r on d.job_id = r.id
    )
    select exists (select 1 from reach where id = new.job_id) into cyc;
    if cyc then
        raise exception 'dependency_cycle: dependency would create a cycle' using errcode = 'DF002';
    end if;
    return new;
end $$;

drop trigger if exists job_dependencies_guard_trg on public.job_dependencies;
create trigger job_dependencies_guard_trg before insert on public.job_dependencies
    for each row execute function public.job_dependencies_guard();

-- ---------------------------------------------------------------- blocked-job reconciliation
-- A job with a (transitive) failed/cancelled dependency becomes needs_review with blocked_by =
-- root cause (smallest id, C collation); it is released again when the cause is gone.
-- Succeeded jobs are not traversed. Mirrors JobStore._reconcile in src/forge/jobs.py.
create or replace function public.reconcile_blocked_jobs() returns int
language plpgsql as $$
declare n1 int := 0; n2 int := 0; n3 int := 0;
begin
    with recursive anc(job_id, anc_id) as (
        select d.job_id, d.depends_on_job_id
          from public.job_dependencies d
          join public.pipeline_jobs a on a.id = d.depends_on_job_id
         where a.status <> 'succeeded'
        union
        select anc.job_id, d.depends_on_job_id
          from anc
          join public.job_dependencies d on d.job_id = anc.anc_id
          join public.pipeline_jobs a on a.id = d.depends_on_job_id
         where a.status <> 'succeeded'
    ), roots as (
        select anc.job_id, (array_agg(anc.anc_id order by anc.anc_id::text collate "C"))[1] as root_id
          from anc join public.pipeline_jobs a on a.id = anc.anc_id
         where a.status in ('permanently_failed','dead_letter','cancelled')
         group by anc.job_id
    )
    update public.pipeline_jobs p
       set status = 'needs_review', blocked_by = r.root_id, updated_at = now(),
           last_error = 'blocked: dependency ' || r.root_id::text || ' is ' ||
                        (select status from public.pipeline_jobs where id = r.root_id)
      from roots r
     where p.id = r.job_id and p.status in ('queued','retryable_failed');
    get diagnostics n1 = row_count;

    with recursive anc(job_id, anc_id) as (
        select d.job_id, d.depends_on_job_id
          from public.job_dependencies d
          join public.pipeline_jobs a on a.id = d.depends_on_job_id
         where a.status <> 'succeeded'
        union
        select anc.job_id, d.depends_on_job_id
          from anc
          join public.job_dependencies d on d.job_id = anc.anc_id
          join public.pipeline_jobs a on a.id = d.depends_on_job_id
         where a.status <> 'succeeded'
    ), roots as (
        select anc.job_id, (array_agg(anc.anc_id order by anc.anc_id::text collate "C"))[1] as root_id
          from anc join public.pipeline_jobs a on a.id = anc.anc_id
         where a.status in ('permanently_failed','dead_letter','cancelled')
         group by anc.job_id
    )
    update public.pipeline_jobs p
       set status = 'queued', blocked_by = null, updated_at = now()
     where p.status = 'needs_review' and p.blocked_by is not null
       and not exists (select 1 from roots r where r.job_id = p.id);
    get diagnostics n2 = row_count;

    with recursive anc(job_id, anc_id) as (
        select d.job_id, d.depends_on_job_id
          from public.job_dependencies d
          join public.pipeline_jobs a on a.id = d.depends_on_job_id
         where a.status <> 'succeeded'
        union
        select anc.job_id, d.depends_on_job_id
          from anc
          join public.job_dependencies d on d.job_id = anc.anc_id
          join public.pipeline_jobs a on a.id = d.depends_on_job_id
         where a.status <> 'succeeded'
    ), roots as (
        select anc.job_id, (array_agg(anc.anc_id order by anc.anc_id::text collate "C"))[1] as root_id
          from anc join public.pipeline_jobs a on a.id = anc.anc_id
         where a.status in ('permanently_failed','dead_letter','cancelled')
         group by anc.job_id
    )
    update public.pipeline_jobs p
       set blocked_by = r.root_id, updated_at = now(),
           last_error = 'blocked: dependency ' || r.root_id::text || ' is ' ||
                        (select status from public.pipeline_jobs where id = r.root_id)
      from roots r
     where p.id = r.job_id and p.status = 'needs_review'
       and p.blocked_by is not null and p.blocked_by <> r.root_id;
    get diagnostics n3 = row_count;
    return n1 + n2 + n3;
end $$;

-- ---------------------------------------------------------------- job functions
-- Idempotent, SCOPED enqueue: identity = (documentary_id, stage, idempotency_key).
-- Same identity + same p_input_hash -> existing row. Same identity + different hash -> DF001.
-- p_input_hash is computed by the client (src/forge/idempotency.py: sha256 of canonical JSON of
-- {"payload": ..., "capability": ...}); p_depends_on is ignored for an existing job.
create or replace function public.enqueue_job(
    p_documentary_id text, p_stage text, p_idempotency_key text, p_input_hash text,
    p_payload jsonb default '{}'::jsonb, p_capability text default 'cpu',
    p_priority int default 100, p_max_attempts int default 3,
    p_depends_on uuid[] default '{}'
) returns public.pipeline_jobs
language plpgsql as $$
declare j public.pipeline_jobs; dep uuid;
begin
    select * into j from public.pipeline_jobs
     where documentary_id = p_documentary_id and stage = p_stage and idempotency_key = p_idempotency_key;
    if found then
        if j.input_hash <> p_input_hash then
            raise exception 'idempotency_conflict: same documentary/stage/key with different inputs' using errcode = 'DF001';
        end if;
        return j;
    end if;
    begin
        insert into public.pipeline_jobs
            (documentary_id, stage, required_capability, payload, idempotency_key, input_hash, priority, max_attempts)
        values (p_documentary_id, p_stage, p_capability, p_payload, p_idempotency_key, p_input_hash, p_priority, p_max_attempts)
        returning * into j;
    exception when unique_violation then
        select * into j from public.pipeline_jobs
         where documentary_id = p_documentary_id and stage = p_stage and idempotency_key = p_idempotency_key;
        if j.input_hash <> p_input_hash then
            raise exception 'idempotency_conflict: same documentary/stage/key with different inputs' using errcode = 'DF001';
        end if;
        return j;
    end;
    if p_depends_on is not null then
        foreach dep in array p_depends_on loop
            insert into public.job_dependencies (job_id, depends_on_job_id) values (j.id, dep)
            on conflict do nothing;
        end loop;
        perform public.reconcile_blocked_jobs();   -- a dependency may already be failed
        select * into j from public.pipeline_jobs where id = j.id;
    end if;
    return j;
end $$;

create or replace function public.add_dependency(p_job uuid, p_depends_on uuid) returns void
language plpgsql as $$
begin
    if p_job = p_depends_on then
        raise exception 'dependency_cycle: a job cannot depend on itself' using errcode = 'DF002';
    end if;
    if exists (select 1 from public.job_dependencies where job_id = p_job and depends_on_job_id = p_depends_on) then
        return;  -- already present (checked first: the guard trigger would reject edits to a started job)
    end if;
    insert into public.job_dependencies (job_id, depends_on_job_id) values (p_job, p_depends_on);
    perform public.reconcile_blocked_jobs();
end $$;

-- Atomically claim one eligible job (0 or 1 rows). attempts after the claim is the fencing token.
create or replace function public.claim_job(
    p_worker text, p_capabilities text[], p_lease_seconds int default 300
) returns setof public.pipeline_jobs
language plpgsql as $$
declare j public.pipeline_jobs; n_dead int;
begin
    if p_lease_seconds <= 0 then
        raise exception 'lease_seconds must be > 0';
    end if;
    -- expired lease on the final attempt -> dead letter
    with dl as (
        update public.pipeline_jobs
           set status = 'dead_letter', lease_owner = null, lease_expires_at = null,
               last_error = coalesce(last_error, '') || ' [lease expired on final attempt]',
               updated_at = now()
         where status in ('claimed','running','checkpointed')
           and lease_expires_at < now() and attempts >= max_attempts
        returning id),
    closed as (
        update public.job_attempts a set ended_at = now(), outcome = 'lease_expired'
          from dl where a.job_id = dl.id and a.ended_at is null
        returning 1)
    select count(*) into n_dead from dl;
    if n_dead > 0 then
        perform public.reconcile_blocked_jobs();
    end if;

    select p.* into j
      from public.pipeline_jobs p
     where p.required_capability = any (p_capabilities)
       and ( (p.status in ('queued','retryable_failed') and p.run_after <= now())
          or (p.status in ('claimed','running','checkpointed')
              and p.lease_expires_at < now() and p.attempts < p.max_attempts) )
       and not exists (select 1 from public.job_dependencies d
                       join public.pipeline_jobs dj on dj.id = d.depends_on_job_id
                       where d.job_id = p.id and dj.status <> 'succeeded')
     order by p.priority, p.seq
     limit 1
       for update of p skip locked;

    if not found then return; end if;

    update public.job_attempts set ended_at = now(), outcome = 'lease_expired'
     where job_id = j.id and ended_at is null;

    update public.pipeline_jobs
       set attempts = attempts + 1, status = 'claimed', lease_owner = p_worker,
           lease_expires_at = now() + make_interval(secs => p_lease_seconds),
           last_heartbeat_at = now(), updated_at = now()
     where id = j.id
    returning * into j;

    insert into public.job_attempts (job_id, attempt, worker_id) values (j.id, j.attempts, p_worker);
    return next j;
end $$;

-- Fenced owner operations: return false/null if the caller no longer owns the job
-- (wrong worker, wrong attempt number, or job not active). Clients must raise LeaseLost.
create or replace function public.heartbeat_job(
    p_job uuid, p_worker text, p_attempt int, p_lease_seconds int default 300
) returns boolean language plpgsql as $$
begin
    update public.pipeline_jobs
       set lease_expires_at = now() + make_interval(secs => p_lease_seconds),
           last_heartbeat_at = now(), updated_at = now(),
           status = case when status = 'claimed' then 'running' else status end
     where id = p_job and lease_owner = p_worker and attempts = p_attempt
       and status in ('claimed','running','checkpointed');
    return found;
end $$;

create or replace function public.checkpoint_job(
    p_job uuid, p_worker text, p_attempt int, p_step text, p_data jsonb, p_lease_seconds int default 300
) returns boolean language plpgsql as $$
begin
    perform 1 from public.pipeline_jobs
      where id = p_job and lease_owner = p_worker and attempts = p_attempt
        and status in ('claimed','running','checkpointed')
      for update;
    if not found then return false; end if;
    insert into public.job_checkpoints (job_id, step, data, attempt)
         values (p_job, p_step, p_data, p_attempt)
    on conflict (job_id, step) do update
        set data = excluded.data, attempt = excluded.attempt, created_at = now();
    update public.pipeline_jobs
       set status = 'checkpointed', last_heartbeat_at = now(), updated_at = now(),
           lease_expires_at = now() + make_interval(secs => p_lease_seconds)
     where id = p_job;
    return true;
end $$;

create or replace function public.complete_job(
    p_job uuid, p_worker text, p_attempt int, p_result jsonb default null
) returns boolean language plpgsql as $$
begin
    update public.pipeline_jobs
       set status = 'succeeded', result = p_result, lease_owner = null,
           lease_expires_at = null, updated_at = now()
     where id = p_job and lease_owner = p_worker and attempts = p_attempt
       and status in ('claimed','running','checkpointed');
    if not found then return false; end if;
    update public.job_attempts set ended_at = now(), outcome = 'succeeded'
     where job_id = p_job and attempt = p_attempt;
    return true;
end $$;

-- p_error MUST already be redacted by the caller (src/forge/redact.py); the database cannot do it.
create or replace function public.fail_job(
    p_job uuid, p_worker text, p_attempt int, p_error text,
    p_retryable boolean default true, p_base_backoff_seconds int default 30
) returns text language plpgsql as $$
declare j public.pipeline_jobs; new_status text; delay_s int;
begin
    select * into j from public.pipeline_jobs
     where id = p_job and lease_owner = p_worker and attempts = p_attempt
       and status in ('claimed','running','checkpointed')
       for update;
    if not found then return null; end if;           -- null = lease lost
    if not p_retryable then new_status := 'permanently_failed';
    elsif j.attempts >= j.max_attempts then new_status := 'dead_letter';
    else new_status := 'retryable_failed'; end if;
    delay_s := least(3600, p_base_backoff_seconds * power(2, j.attempts - 1))::int;
    update public.pipeline_jobs
       set status = new_status, last_error = left(p_error, 2000),
           run_after = case when new_status = 'retryable_failed'
                            then now() + make_interval(secs => delay_s) else now() end,
           lease_owner = null, lease_expires_at = null, updated_at = now()
     where id = p_job;
    update public.job_attempts set ended_at = now(), outcome = new_status, error = left(p_error, 2000)
     where job_id = p_job and attempt = p_attempt;
    if new_status in ('permanently_failed','dead_letter') then
        perform public.reconcile_blocked_jobs();
    end if;
    return new_status;
end $$;

-- Operator actions ---------------------------------------------------------
create or replace function public.cancel_job(p_job uuid, p_reason text default null) returns boolean
language plpgsql as $$
begin
    update public.pipeline_jobs
       set status = 'cancelled', lease_owner = null, lease_expires_at = null, blocked_by = null,
           last_error = coalesce(left(p_reason, 2000), last_error), updated_at = now()
     where id = p_job and status not in ('succeeded','cancelled');
    if not found then return false; end if;
    update public.job_attempts set ended_at = now(), outcome = 'cancelled'
     where job_id = p_job and ended_at is null;
    perform public.reconcile_blocked_jobs();
    return true;
end $$;

-- attempts is never reset (it is the fencing token); max_attempts = attempts + extra.
create or replace function public.requeue_job(p_job uuid, p_extra_attempts int default 3)
returns setof public.pipeline_jobs language plpgsql as $$
declare j public.pipeline_jobs;
begin
    if p_extra_attempts < 1 then
        raise exception 'extra_attempts must be >= 1';
    end if;
    update public.pipeline_jobs
       set status = 'queued', max_attempts = attempts + p_extra_attempts, run_after = now(),
           blocked_by = null, lease_owner = null, lease_expires_at = null, updated_at = now()
     where id = p_job and status in ('permanently_failed','dead_letter','cancelled','needs_review');
    if not found then return; end if;
    perform public.reconcile_blocked_jobs();
    select * into j from public.pipeline_jobs where id = p_job;
    return next j;
end $$;

-- External-operation ledger ------------------------------------------------
-- begin_operation: fenced. Returns the ledger row (0 rows = lease lost). 'committed' means done:
-- do not execute again. executions > 1 while 'started' means an earlier attempt may have produced
-- the effect: probe the external system, or re-execute only if idempotent under operation_id.
create or replace function public.begin_operation(
    p_job uuid, p_worker text, p_attempt int,
    p_operation_id text, p_op_name text, p_input_hash text
) returns setof public.external_operations language plpgsql as $$
declare jb public.pipeline_jobs; op public.external_operations;
begin
    select * into jb from public.pipeline_jobs
     where id = p_job and lease_owner = p_worker and attempts = p_attempt
       and status in ('claimed','running','checkpointed')
       for update;
    if not found then return; end if;
    insert into public.external_operations
        (operation_id, documentary_id, stage, op_name, input_hash, job_id, last_worker, last_attempt)
    values (p_operation_id, jb.documentary_id, jb.stage, p_op_name, p_input_hash, p_job, p_worker, p_attempt)
    on conflict (operation_id) do update
        set executions   = public.external_operations.executions
                           + (public.external_operations.status = 'started')::int,
            job_id       = case when public.external_operations.status = 'started' then excluded.job_id
                                else public.external_operations.job_id end,
            last_worker  = case when public.external_operations.status = 'started' then excluded.last_worker
                                else public.external_operations.last_worker end,
            last_attempt = case when public.external_operations.status = 'started' then excluded.last_attempt
                                else public.external_operations.last_attempt end,
            updated_at   = case when public.external_operations.status = 'started' then now()
                                else public.external_operations.updated_at end
    returning * into op;
    if op.input_hash <> p_input_hash then
        raise exception 'operation_conflict: operation id collision with different input' using errcode = 'DF004';
    end if;
    return next op;
end $$;

-- commit_operation: fenced. Returns the row (0 rows = lease lost). Re-commit with the same
-- output_hash is a no-op; with a different output_hash raises DF004.
create or replace function public.commit_operation(
    p_job uuid, p_worker text, p_attempt int,
    p_operation_id text, p_output jsonb, p_output_hash text
) returns setof public.external_operations language plpgsql as $$
declare jb public.pipeline_jobs; op public.external_operations;
begin
    select * into jb from public.pipeline_jobs
     where id = p_job and lease_owner = p_worker and attempts = p_attempt
       and status in ('claimed','running','checkpointed')
       for update;
    if not found then return; end if;
    select * into op from public.external_operations where operation_id = p_operation_id for update;
    if not found or op.documentary_id <> jb.documentary_id or op.stage <> jb.stage then
        raise exception 'unknown operation for this job';
    end if;
    if op.status = 'committed' then
        if op.output_hash <> p_output_hash then
            raise exception 'operation_conflict: already committed with a different output' using errcode = 'DF004';
        end if;
    else
        update public.external_operations
           set status = 'committed', output = p_output, output_hash = p_output_hash,
               committed_at = now(), updated_at = now()
         where operation_id = p_operation_id
        returning * into op;
    end if;
    return next op;
end $$;

-- ---------------------------------------------------------------- operator view
create or replace view public.blocked_jobs as
select id as job_id, documentary_id, stage, status, blocked_by, last_error as detail,
       case when blocked_by is not null then 'dependency_failed' else 'review_required' end as reason,
       updated_at as since
  from public.pipeline_jobs
 where status = 'needs_review';

-- ---------------------------------------------------------------- privileges
do $$
declare f text;
begin
    foreach f in array array[
        'job_dependencies_guard()',
        'reconcile_blocked_jobs()',
        'enqueue_job(text,text,text,text,jsonb,text,int,int,uuid[])',
        'add_dependency(uuid,uuid)',
        'claim_job(text,text[],int)',
        'heartbeat_job(uuid,text,int,int)',
        'checkpoint_job(uuid,text,int,text,jsonb,int)',
        'complete_job(uuid,text,int,jsonb)',
        'fail_job(uuid,text,int,text,boolean,int)',
        'cancel_job(uuid,text)',
        'requeue_job(uuid,int)',
        'begin_operation(uuid,text,int,text,text,text)',
        'commit_operation(uuid,text,int,text,jsonb,text)'
    ] loop
        execute format('revoke all on function public.%s from public', f);
        if exists (select 1 from pg_roles where rolname = 'anon') then
            execute format('revoke all on function public.%s from anon, authenticated', f);
        end if;
        if exists (select 1 from pg_roles where rolname = 'service_role') then
            execute format('grant execute on function public.%s to service_role', f);
        end if;
    end loop;
    execute 'revoke all on public.blocked_jobs from public';
    if exists (select 1 from pg_roles where rolname = 'anon') then
        execute 'revoke all on public.blocked_jobs from anon, authenticated';
    end if;
    if exists (select 1 from pg_roles where rolname = 'service_role') then
        execute 'grant select on public.blocked_jobs to service_role';
    end if;
end $$;
