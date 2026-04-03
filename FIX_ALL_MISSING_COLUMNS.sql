-- SOCIO COMPLETE DATABASE SCHEMA FOR SUPABASE
-- CHRIST University Event Management Platform
-- Apply this entire script in: https://app.supabase.com/project/wvebxdbvoinylwecmisv/sql/new
-- Run this ONCE in Supabase SQL Editor. Paste everything below and click Run.

-- ============================================================
-- COMPLETE SOCIO DATABASE SCHEMA
-- ============================================================

create extension if not exists pgcrypto;

-- Canonicalize legacy table name if needed
do $$
begin
  if to_regclass('public.fest') is not null and to_regclass('public.fests') is null then
    alter table public.fest rename to fests;
  end if;
end $$;

-- ============================================================
-- CORE TABLES
-- ============================================================

create table if not exists public.users (
  id uuid primary key default gen_random_uuid(),
  auth_uuid uuid unique,
  email text unique not null,
  name text,
  avatar_url text,
  is_organiser boolean not null default false,
  organiser_expires_at timestamptz,
  is_support boolean not null default false,
  support_expires_at timestamptz,
  is_masteradmin boolean not null default false,
  masteradmin_expires_at timestamptz,
  course text,
  register_number text,
  organization_type text not null default 'christ_member' check (organization_type in ('christ_member', 'outsider')),
  visitor_id text,
  outsider_name_edit_used boolean not null default false,
  department_id uuid,
  campus text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.departments_courses (
  id uuid primary key default gen_random_uuid(),
  department_name text unique not null,
  school text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.classes (
  id uuid primary key default gen_random_uuid(),
  class_name text not null,
  department_id uuid not null references public.departments_courses(id) on delete cascade,
  created_at timestamptz not null default now()
);

create table if not exists public.fests (
  id uuid primary key default gen_random_uuid(),
  auth_uuid text,
  fest_id text unique not null,
  fest_title text not null,
  description text,
  opening_date date,
  closing_date date,
  fest_image_url text,
  venue text,
  status text,
  registration_deadline timestamptz,
  organizing_dept text,
  department_access jsonb not null default '[]'::jsonb,
  category text,
  contact_email text,
  contact_phone text,
  event_heads jsonb not null default '[]'::jsonb,
  timeline jsonb not null default '[]'::jsonb,
  sponsors jsonb not null default '[]'::jsonb,
  social_links jsonb not null default '[]'::jsonb,
  faqs jsonb not null default '[]'::jsonb,
  campus_hosted_at text,
  allowed_campuses jsonb not null default '[]'::jsonb,
  allow_outsiders boolean not null default false,
  created_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.events (
  id uuid primary key default gen_random_uuid(),
  auth_uuid text,
  event_id text unique not null,
  title text not null,
  description text,
  event_date date,
  end_date date,
  event_time time,
  venue text,
  category text,
  department_access jsonb not null default '[]'::jsonb,
  organizing_dept text,
  claims_applicable boolean not null default false,
  registration_fee numeric,
  outsider_registration_fee numeric,
  participants_per_team integer,
  max_participants integer,
  outsider_max_participants integer,
  total_participants integer not null default 0,
  event_image_url text,
  banner_url text,
  pdf_url text,
  rules jsonb not null default '[]'::jsonb,
  schedule jsonb not null default '[]'::jsonb,
  prizes jsonb not null default '[]'::jsonb,
  custom_fields jsonb not null default '[]'::jsonb,
  organizer_email text,
  organizer_phone text,
  whatsapp_invite_link text,
  registration_deadline timestamptz,
  allow_outsiders boolean not null default false,
  campus_hosted_at text,
  allowed_campuses jsonb not null default '[]'::jsonb,
  fest_id text,
  fest text,
  created_by text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint fk_events_fest_id foreign key (fest_id) references public.fests(fest_id) on update cascade on delete set null
);

create table if not exists public.registrations (
  id uuid primary key default gen_random_uuid(),
  registration_id text unique not null,
  event_id text,
  user_email text,
  register_id text,
  registration_type text check (registration_type in ('individual', 'team')),
  individual_name text,
  individual_email text,
  individual_register_number text,
  team_name text,
  team_leader_name text,
  team_leader_email text,
  team_leader_register_number text,
  teammates jsonb not null default '[]'::jsonb,
  participant_organization text not null default 'christ_member' check (participant_organization in ('christ_member', 'outsider')),
  qr_code_data jsonb,
  qr_code_generated_at timestamptz,
  custom_field_responses jsonb,
  created_at timestamptz not null default now(),
  constraint fk_registrations_event_id foreign key (event_id) references public.events(event_id) on update cascade on delete cascade
);

create table if not exists public.attendance_status (
  id uuid primary key default gen_random_uuid(),
  registration_id text unique not null,
  event_id text,
  status text check (status in ('attended', 'absent', 'pending')),
  marked_at timestamptz not null default now(),
  marked_by text
);

create table if not exists public.notifications (
  id uuid primary key default gen_random_uuid(),
  user_email text,
  title text not null,
  message text,
  type text,
  read boolean not null default false,
  event_id text,
  event_title text,
  action_url text,
  is_broadcast boolean not null default false,
  created_at timestamptz not null default now()
);

create table if not exists public.notification_user_status (
  id uuid primary key default gen_random_uuid(),
  notification_id uuid not null,
  user_email text not null,
  is_read boolean not null default false,
  is_dismissed boolean not null default false,
  created_at timestamptz not null default now(),
  constraint fk_notification_user_status_notification_id foreign key (notification_id) references public.notifications(id) on delete cascade
);

create table if not exists public.contact_messages (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  email text not null,
  subject text not null,
  message text not null,
  source text default 'contact',
  status text default 'new',
  handled_by uuid,
  handled_at timestamptz,
  created_at timestamptz not null default now()
);

create table if not exists public.qr_scan_logs (
  id uuid primary key default gen_random_uuid(),
  registration_id text,
  event_id text,
  scanned_by text,
  scan_timestamp timestamptz not null default now(),
  scan_result text,
  scanner_info jsonb
);

-- ============================================================
-- TRIGGERS & FUNCTIONS
-- ============================================================

create or replace function public.sync_event_fest_fields() returns trigger language plpgsql as $$
begin
  if new.fest_id is null and new.fest is not null then
    new.fest_id := new.fest;
  elsif new.fest is null and new.fest_id is not null then
    new.fest := new.fest_id;
  elsif new.fest_id is not null and new.fest is not null and new.fest <> new.fest_id then
    new.fest := new.fest_id;
  end if;
  return new;
end;
$$;

drop trigger if exists trg_sync_event_fest_fields on public.events;
create trigger trg_sync_event_fest_fields before insert or update on public.events for each row execute function public.sync_event_fest_fields();

create or replace function public.set_updated_at() returns trigger language plpgsql as $$
begin
  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists trg_users_updated_at on public.users;
create trigger trg_users_updated_at before update on public.users for each row execute function public.set_updated_at();

drop trigger if exists trg_fests_updated_at on public.fests;
create trigger trg_fests_updated_at before update on public.fests for each row execute function public.set_updated_at();

drop trigger if exists trg_events_updated_at on public.events;
create trigger trg_events_updated_at before update on public.events for each row execute function public.set_updated_at();

drop trigger if exists trg_departments_updated_at on public.departments_courses;
create trigger trg_departments_updated_at before update on public.departments_courses for each row execute function public.set_updated_at();

-- ============================================================
-- INDEXES
-- ============================================================

create unique index if not exists idx_users_visitor_id on public.users(visitor_id) where visitor_id is not null;
create index if not exists idx_users_created_at on public.users(created_at desc);
create index if not exists idx_users_email_lower on public.users((lower(email)));
create index if not exists idx_users_roles on public.users(is_organiser, is_support, is_masteradmin);
create index if not exists idx_users_department_id on public.users(department_id);
create index if not exists idx_users_campus on public.users(campus);

create index if not exists idx_departments_courses_name on public.departments_courses(lower(department_name));
create index if not exists idx_classes_department_id on public.classes(department_id);
create index if not exists idx_classes_name on public.classes(class_name);

create index if not exists idx_fests_created_at on public.fests(created_at desc);
create index if not exists idx_fests_opening_date on public.fests(opening_date desc);
create index if not exists idx_fests_title_lower on public.fests((lower(fest_title)));

create index if not exists idx_events_created_at on public.events(created_at desc);
create index if not exists idx_events_event_date on public.events(event_date desc);
create index if not exists idx_events_title_lower on public.events((lower(title)));
create index if not exists idx_events_fest_id on public.events(fest_id);
create index if not exists idx_events_campus_hosted_at on public.events(campus_hosted_at);
create index if not exists idx_events_allowed_campuses on public.events using gin(allowed_campuses);

create index if not exists idx_registrations_event_id on public.registrations(event_id);
create index if not exists idx_registrations_user_email on public.registrations(user_email);
create index if not exists idx_registrations_custom_fields on public.registrations using gin(custom_field_responses);

create index if not exists idx_attendance_event_id on public.attendance_status(event_id);
create index if not exists idx_attendance_registration_id on public.attendance_status(registration_id);

create index if not exists idx_notifications_user_read_created on public.notifications(user_email, read, created_at desc);
create index if not exists idx_notification_user_status_user_email on public.notification_user_status(user_email);
create index if not exists idx_notification_user_status_notification_id on public.notification_user_status(notification_id);

-- ============================================================
-- RLS (ROW LEVEL SECURITY)
-- ============================================================

alter table public.users enable row level security;
alter table public.fests enable row level security;
alter table public.events enable row level security;
alter table public.registrations enable row level security;
alter table public.attendance_status enable row level security;
alter table public.notifications enable row level security;
alter table public.contact_messages enable row level security;
alter table public.qr_scan_logs enable row level security;
alter table public.departments_courses enable row level security;
alter table public.classes enable row level security;
alter table public.notification_user_status enable row level security;

drop policy if exists "Allow all access to users" on public.users;
create policy "Allow all access to users" on public.users for all using (true) with check (true);

drop policy if exists "Allow all access to fests" on public.fests;
create policy "Allow all access to fests" on public.fests for all using (true) with check (true);

drop policy if exists "Allow all access to events" on public.events;
create policy "Allow all access to events" on public.events for all using (true) with check (true);

drop policy if exists "Allow all access to registrations" on public.registrations;
create policy "Allow all access to registrations" on public.registrations for all using (true) with check (true);

drop policy if exists "Allow all access to attendance_status" on public.attendance_status;
create policy "Allow all access to attendance_status" on public.attendance_status for all using (true) with check (true);

drop policy if exists "Allow all access to notifications" on public.notifications;
create policy "Allow all access to notifications" on public.notifications for all using (true) with check (true);

drop policy if exists "Allow all access to contact_messages" on public.contact_messages;
create policy "Allow all access to contact_messages" on public.contact_messages for all using (true) with check (true);

drop policy if exists "Allow all access to qr_scan_logs" on public.qr_scan_logs;
create policy "Allow all access to qr_scan_logs" on public.qr_scan_logs for all using (true) with check (true);

drop policy if exists "Allow all access to departments_courses" on public.departments_courses;
create policy "Allow all access to departments_courses" on public.departments_courses for all using (true) with check (true);

drop policy if exists "Allow all access to classes" on public.classes;
create policy "Allow all access to classes" on public.classes for all using (true) with check (true);

drop policy if exists "Allow all access to notification_user_status" on public.notification_user_status;
create policy "Allow all access to notification_user_status" on public.notification_user_status for all using (true) with check (true);

-- ============================================================
-- ADD FOREIGN KEY FOR USERS -> DEPARTMENTS
-- ============================================================

do $$
begin
  if not exists (
    select 1 from information_schema.table_constraints 
    where constraint_name = 'fk_users_department'
  ) then
    alter table public.users
    add constraint fk_users_department
      foreign key (department_id)
      references public.departments_courses(id)
      on update cascade
      on delete set null;
  end if;
end $$;

-- ============================================================
-- VERIFICATION QUERIES
-- ============================================================

SELECT 'Core Tables' as check_type,
       COUNT(*) as table_count
FROM information_schema.tables
WHERE table_schema = 'public' 
AND table_name IN (
  'users', 'fests', 'events', 'registrations', 'attendance_status', 
  'notifications', 'notification_user_status', 'contact_messages', 
  'qr_scan_logs', 'departments_courses', 'classes'
);

SELECT 'users.campus' as column_checked, 
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='campus') as exists;

SELECT 'events.campus_hosted_at' as column_checked, 
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='events' AND column_name='campus_hosted_at') as exists;

SELECT 'events.allowed_campuses' as column_checked, 
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='events' AND column_name='allowed_campuses') as exists;

SELECT 'registrations.custom_field_responses' as column_checked, 
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='registrations' AND column_name='custom_field_responses') as exists;

SELECT 'notifications.is_broadcast' as column_checked, 
  EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='notifications' AND column_name='is_broadcast') as exists;

SELECT 'notification_user_status table exists' as table_checked, 
  EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name='notification_user_status') as exists;
