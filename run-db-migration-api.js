#!/usr/bin/env node

const https = require('https');
require('dotenv').config({ path: './server/.env' });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env');
  process.exit(1);
}

// SQL statements to execute
const statements = [
  `ALTER TABLE notifications ADD COLUMN IF NOT EXISTS is_broadcast BOOLEAN DEFAULT false;`,
  
  `CREATE TABLE IF NOT EXISTS public.notification_user_status (
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
  );`,
  
  `CREATE INDEX IF NOT EXISTS idx_notification_user_status_user_email ON public.notification_user_status(user_email);`,
  
  `CREATE INDEX IF NOT EXISTS idx_notification_user_status_notification_id ON public.notification_user_status(notification_id);`,
  
  `ALTER TABLE public.notification_user_status ENABLE ROW LEVEL SECURITY;`,
  
  `DROP POLICY IF EXISTS "Allow all access to notification_user_status" ON public.notification_user_status;`,
  
  `CREATE POLICY "Allow all access to notification_user_status" ON public.notification_user_status FOR ALL USING (true) WITH CHECK (true);`,
  
  `SELECT 'is_broadcast column exists: ' || CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.columns 
    WHERE table_name = 'notifications' AND column_name = 'is_broadcast'
  ) THEN 'YES' ELSE 'NO' END;`,
  
  `SELECT 'notification_user_status table exists: ' || CASE WHEN EXISTS (
    SELECT 1 FROM information_schema.tables 
    WHERE table_name = 'notification_user_status'
  ) THEN 'YES' ELSE 'NO' END;`
];

function executeSQL(sql) {
  return new Promise((resolve, reject) => {
    const url = new URL(`${SUPABASE_URL}/rest/v1/rpc/exec`);
    
    const data = JSON.stringify({
      sql: sql
    });

    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data),
        'Authorization': `Bearer ${SERVICE_ROLE_KEY}`,
        'apikey': SERVICE_ROLE_KEY
      }
    };

    const req = https.request(options, (res) => {
      let responseData = '';

      res.on('data', (chunk) => {
        responseData += chunk;
      });

      res.on('end', () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          resolve(responseData);
        } else {
          reject(new Error(`Status ${res.statusCode}: ${responseData}`));
        }
      });
    });

    req.on('error', (e) => {
      reject(e);
    });

    req.write(data);
    req.end();
  });
}

async function runMigration() {
  try {
    console.log('🚀 Starting database migration via Supabase API...\n');

    for (let i = 0; i < statements.length; i++) {
      const statement = statements[i];
      console.log(`Step ${i + 1}/${statements.length}: ${statement.substring(0, 50)}...`);

      try {
        const result = await executeSQL(statement);
        console.log('   ✅ Success\n');
      } catch (err) {
        console.log(`   ⚠️  ${err.message}\n`);
      }
    }

    console.log('🎉 Database migration completed!\n');

  } catch (err) {
    console.error('❌ Migration failed:', err.message);
    process.exit(1);
  }
}

runMigration();
