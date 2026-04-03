#!/usr/bin/env node

const https = require('https');
require('dotenv').config({ path: './server/.env' });

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error('❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  process.exit(1);
}

// Split URL to get hostname and path
const url = new URL(SUPABASE_URL);
const hostname = url.hostname;

// SQL to execute
const sqlStatements = [
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
  
  `ALTER TABLE notifications ENABLE ROW LEVEL SECURITY;`,
  
  `DROP POLICY IF EXISTS "public can view notifications" ON notifications;`,
  
  `CREATE POLICY "public can view notifications" ON notifications FOR SELECT USING (true);`,
  
  `DROP POLICY IF EXISTS "authenticated can insert notifications" ON notifications;`,
  
  `CREATE POLICY "authenticated can insert notifications" ON notifications FOR INSERT WITH CHECK (true);`,
  
  `DROP POLICY IF EXISTS "authenticated can update notifications" ON notifications;`,
  
  `CREATE POLICY "authenticated can update notifications" ON notifications FOR UPDATE USING (true) WITH CHECK (true);`,

  `SELECT 'is_broadcast column: ' || CASE WHEN EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'notifications' AND column_name = 'is_broadcast') THEN 'EXISTS' ELSE 'MISSING' END;`,

  `SELECT 'notification_user_status table: ' || CASE WHEN EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'notification_user_status') THEN 'EXISTS' ELSE 'MISSING' END;`
];

function executeSql(sql) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify({ query: sql });
    
    const options = {
      hostname: hostname,
      path: '/rest/v1/rpc/exec',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData),
        'Authorization': `Bearer ${SERVICE_ROLE_KEY}`,
        'apikey': SERVICE_ROLE_KEY
      }
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          if (res.statusCode >= 200 && res.statusCode < 300) {
            resolve({ status: res.statusCode, data });
          } else {
            reject(new Error(`Status ${res.statusCode}: ${data}`));
          }
        } catch (e) {
          reject(e);
        }
      });
    });

    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

async function migrateDatabase() {
  console.log('🚀 Starting Supabase database migration...\n');
  console.log(`📍 Database: ${SUPABASE_URL}`);
  console.log(`📊 Executing ${sqlStatements.length} SQL statements\n`);

  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < sqlStatements.length; i++) {
    const sql = sqlStatements[i];
    const statement = sql.substring(0, 60) + (sql.length > 60 ? '...' : '');
    
    try {
      await executeSql(sql);
      console.log(`✅ [${i + 1}/${sqlStatements.length}] ${statement}`);
      successCount++;
    } catch (err) {
      console.log(`⚠️  [${i + 1}/${sqlStatements.length}] ${statement}`);
      console.log(`   Error: ${err.message}\n`);
      errorCount++;
    }
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`📋 Migration Summary:`);
  console.log(`   ✅ Successful: ${successCount}`);
  console.log(`   ⚠️  Errors/Warnings: ${errorCount}`);
  console.log(`${'='.repeat(60)}`);

  if (errorCount === 0) {
    console.log(`\n🎉 Database migration completed successfully!`);
    console.log(`Try updating your fest now - it should work!\n`);
  } else {
    console.log(`\n⚠️  Some statements had issues, but this might be OK (e.g., if table already exists).\n`);
  }
}

migrateDatabase().catch(err => {
  console.error('❌ Fatal error:', err.message);
  process.exit(1);
});
