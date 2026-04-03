# APPLY THIS IN SUPABASE IMMEDIATELY

**Step 1:** Go to https://app.supabase.com/project/wvebxdbvoinylwecmisv/sql/new

**Step 2:** Copy and paste THIS entire script:

```sql
-- Add is_broadcast column to notifications table
ALTER TABLE notifications 
ADD COLUMN IF NOT EXISTS is_broadcast BOOLEAN DEFAULT false;

-- Create notification_user_status table
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

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_notification_user_status_user_email 
  ON public.notification_user_status(user_email);

CREATE INDEX IF NOT EXISTS idx_notification_user_status_notification_id 
  ON public.notification_user_status(notification_id);

-- Enable Row Level Security
ALTER TABLE public.notification_user_status ENABLE ROW LEVEL SECURITY;

-- Create policy
DROP POLICY IF EXISTS "Allow all access to notification_user_status" 
  ON public.notification_user_status;

CREATE POLICY "Allow all access to notification_user_status" 
  ON public.notification_user_status 
  FOR ALL 
  USING (true) 
  WITH CHECK (true);

-- Fix notifications table RLS if needed
ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;

-- Allow all authenticated users to read notifications
DROP POLICY IF EXISTS "public can view notifications" ON notifications;

CREATE POLICY "public can view notifications" 
  ON notifications 
  FOR SELECT 
  USING (true);

-- Allow authenticated users to insert notifications  
DROP POLICY IF EXISTS "authenticated can insert notifications" ON notifications;

CREATE POLICY "authenticated can insert notifications"
  ON notifications
  FOR INSERT
  WITH CHECK (true);

-- Allow updates
DROP POLICY IF EXISTS "authenticated can update notifications" ON notifications;

CREATE POLICY "authenticated can update notifications"
  ON notifications
  FOR UPDATE
  USING (true)
  WITH CHECK (true);

-- Verify it worked
SELECT 'is_broadcast column: ' || CASE WHEN EXISTS (
  SELECT 1 FROM information_schema.columns 
  WHERE table_name = 'notifications' AND column_name = 'is_broadcast'
) THEN '✅ EXISTS' ELSE '❌ MISSING' END as status;

SELECT 'notification_user_status table: ' || CASE WHEN EXISTS (
  SELECT 1 FROM information_schema.tables 
  WHERE table_name = 'notification_user_status'
) THEN '✅ EXISTS' ELSE '❌ MISSING' END as status;
```

**Step 3:** Click **Run** (or Ctrl+Enter)

**Step 4:** You should see two SELECT results confirming both exist

That's it! Then try updating the fest again.
