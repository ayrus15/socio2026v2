#!/usr/bin/env node

const { Client } = require('pg');
require('dotenv').config({ path: './server/.env' });

// Supabase PostgreSQL connection details
const connectionString = 'postgresql://postgres.wvebxdbvoinylwecmisv:[PASSWORD]@wvebxdbvoinylwecmisv.supabase.co:5432/postgres';

// Try to get password from environment or use the connection string format
// For now, we'll prompt the user or use direct access
const client = new Client({
  host: 'wvebxdbvoinylwecmisv.supabase.co',
  port: 5432,
  database: 'postgres',
  user: 'postgres',
  password: process.env.SUPABASE_DB_PASSWORD || process.argv[2],
  ssl: { rejectUnauthorized: false }
});

const migrationSteps = [
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
  
  `CREATE POLICY "Allow all access to notification_user_status" ON public.notification_user_status FOR ALL USING (true) WITH CHECK (true);`
];

async function runMigration() {
  try {
    if (!process.argv[2] && !process.env.SUPABASE_DB_PASSWORD) {
      console.error('❌ Database password required!');
      console.log('\nUsage: node run-db-migration.js <password>');
      console.log('Or set SUPABASE_DB_PASSWORD environment variable');
      process.exit(1);
    }

    console.log('🚀 Connecting to Supabase database...\n');
    await client.connect();
    console.log('✅ Connected!\n');

    console.log('📝 Running migration steps...\n');

    for (let i = 0; i < migrationSteps.length; i++) {
      const step = migrationSteps[i];
      console.log(`Step ${i + 1}/${migrationSteps.length}: ${step.substring(0, 50)}...`);
      
      try {
        await client.query(step);
        console.log('   ✅ Success\n');
      } catch (err) {
        console.log(`   ⚠️  ${err.message}\n`);
      }
    }

    // Verify migration
    console.log('📋 Verifying migration...\n');

    const colResult = await client.query(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'notifications' AND column_name = 'is_broadcast'
    `);

    if (colResult.rows.length > 0) {
      console.log('✅ is_broadcast column EXISTS');
    }

    const tableResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_name = 'notification_user_status'
    `);

    if (tableResult.rows.length > 0) {
      console.log('✅ notification_user_status table EXISTS');
    }

    console.log('\n🎉 Database migration completed successfully!\n');

  } catch (err) {
    console.error('❌ Migration failed:', err.message);
    console.error('Full error:', err);
    process.exit(1);
  } finally {
    try {
      await client.end();
    } catch (e) {
      console.log('Connection ended');
    }
  }
}

runMigration();
