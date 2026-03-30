const FEST_TABLE_CANDIDATES = ["fests", "fest"];

let cachedDatabaseFestTable = null;
let cachedSupabaseFestTable = null;

const isMissingRelationError = (error) => {
  const message = String(error?.message || "").toLowerCase();
  return error?.code === "42P01" || message.includes("relation") && message.includes("does not exist");
};

export async function getFestTableForDatabase(queryAllFn) {
  if (cachedDatabaseFestTable) {
    return cachedDatabaseFestTable;
  }

  let lastError = null;
  for (const tableName of FEST_TABLE_CANDIDATES) {
    try {
      await queryAllFn(tableName, { select: "fest_id", limit: 1 });
      cachedDatabaseFestTable = tableName;
      return tableName;
    } catch (error) {
      if (isMissingRelationError(error)) {
        lastError = error;
        continue;
      }
      throw error;
    }
  }

  throw lastError || new Error("Unable to resolve fest table name");
}

export async function getFestTableForSupabase(supabaseClient) {
  if (cachedSupabaseFestTable) {
    return cachedSupabaseFestTable;
  }

  let lastError = null;
  for (const tableName of FEST_TABLE_CANDIDATES) {
    const { error } = await supabaseClient
      .from(tableName)
      .select("fest_id")
      .limit(1);

    if (!error) {
      cachedSupabaseFestTable = tableName;
      return tableName;
    }

    if (isMissingRelationError(error)) {
      lastError = error;
      continue;
    }

    throw error;
  }

  throw lastError || new Error("Unable to resolve fest table name");
}
