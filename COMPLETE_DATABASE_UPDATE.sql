-- Complete Notifications Database Update
-- Run this entire script in Supabase SQL Editor

-- Step 1: Add is_broadcast column to notifications table
ALTER TABLE notifications 
ADD COLUMN IF NOT EXISTS is_broadcast BOOLEAN DEFAULT false;

-- Step 2: Create notification_user_status table
CREATE TABLE IF NOT EXISTS public.notification_user_status (
  id uuid primary key default gen_random_uuid(),
  notification_id uuid not null,
  user_email text not null,
  is_read boolean not null default false,
  is_dismissed boolean not null default false,
  created_at timestamptz not null default now(),

  constraint fk_notification_user_status_notification_id
    foreign key (notification_id)
    references public.notifications(id)
    on delete cascade
);

-- Step 3: Create indexes
CREATE INDEX IF NOT EXISTS idx_notification_user_status_user_email 
  ON public.notification_user_status(user_email);

CREATE INDEX IF NOT EXISTS idx_notification_user_status_notification_id 
  ON public.notification_user_status(notification_id);

-- Step 4: Enable Row Level Security
ALTER TABLE public.notification_user_status ENABLE ROW LEVEL SECURITY;

-- Step 5: Drop existing policy if it exists, then create new one
DROP POLICY IF EXISTS "Allow all access to notification_user_status" 
  ON public.notification_user_status;

CREATE POLICY "Allow all access to notification_user_status" 
  ON public.notification_user_status 
  FOR ALL 
  USING (true) 
  WITH CHECK (true);

-- Done! Verify tables exist
SELECT 'notification_user_status table status: ' || CASE WHEN EXISTS (
  SELECT 1 FROM information_schema.tables 
  WHERE table_name = 'notification_user_status'
) THEN 'EXISTS' ELSE 'MISSING' END as status;

SELECT 'is_broadcast column status: ' || CASE WHEN EXISTS (
  SELECT 1 FROM information_schema.columns 
  WHERE table_name = 'notifications' AND column_name = 'is_broadcast'
) THEN 'EXISTS' ELSE 'MISSING' END as status;
