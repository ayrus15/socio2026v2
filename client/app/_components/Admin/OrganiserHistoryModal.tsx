"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Building2, CalendarDays, FolderKanban, History, Loader2, X } from "lucide-react";
import { createBrowserClient } from "@supabase/ssr";
import { useAuth } from "@/context/AuthContext";

type OrganiserEvent = {
  event_id: string;
  title: string;
  event_date: string;
  fest?: string | null;
  organizing_dept?: string | null;
  created_by: string;
  created_at: string;
};

type OrganiserHistoryModalProps = {
  isOpen: boolean;
  organiserIdentifier: string | null;
  organiserOptions: string[];
  onOrganiserChange: (identifier: string | null) => void;
  onClose: () => void;
};

const formatDate = (value: string) => {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "Unavailable";
  return parsed.toLocaleDateString("en-IN", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
};

const getEventStatus = (eventDate: string) => {
  const parsed = new Date(eventDate);
  if (Number.isNaN(parsed.getTime())) {
    return {
      label: "Unknown",
      className: "bg-slate-100 text-slate-600 border border-slate-200",
    };
  }

  if (parsed.getTime() < Date.now()) {
    return {
      label: "Past",
      className: "bg-slate-100 text-slate-700 border border-slate-200",
    };
  }

  return {
    label: "Upcoming",
    className: "bg-emerald-50 text-emerald-700 border border-emerald-200",
  };
};

export default function OrganiserHistoryModal({
  isOpen,
  organiserIdentifier,
  organiserOptions,
  onOrganiserChange,
  onClose,
}: OrganiserHistoryModalProps) {
  const { session } = useAuth();
  const [events, setEvents] = useState<OrganiserEvent[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeIdentifier, setActiveIdentifier] = useState("");

  const supabase = useMemo(
    () =>
      createBrowserClient(
        process.env.NEXT_PUBLIC_SUPABASE_URL!,
        process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
      ),
    []
  );

  const getEventsByOrganiser = useCallback(
    async (identifier: string) => {
      if (session?.access_token && session?.refresh_token) {
        await supabase.auth.setSession({
          access_token: session.access_token,
          refresh_token: session.refresh_token,
        });
      }

      const { data, error: fetchError } = await supabase
        .from("events")
        .select(
          "event_id, title, event_date, fest, organizing_dept, created_by, created_at"
        )
        .eq("created_by", identifier)
        .order("created_at", { ascending: false });

      if (fetchError) {
        throw fetchError;
      }

      return (data ?? []) as OrganiserEvent[];
    },
    [session?.access_token, session?.refresh_token, supabase]
  );

  useEffect(() => {
    if (!isOpen) return;

    if (organiserIdentifier) {
      setActiveIdentifier(organiserIdentifier);
      return;
    }

    setActiveIdentifier("");
  }, [isOpen, organiserIdentifier]);

  useEffect(() => {
    if (!isOpen) return;

    if (!activeIdentifier) {
      setEvents([]);
      setError(null);
      setIsLoading(false);
      return;
    }

    let alive = true;
    const load = async () => {
      setIsLoading(true);
      setError(null);

      try {
        const result = await getEventsByOrganiser(activeIdentifier);
        if (alive) {
          setEvents(result);
        }
      } catch (err: any) {
        if (alive) {
          setEvents([]);
          setError(err?.message || "Failed to fetch organiser history");
        }
      } finally {
        if (alive) {
          setIsLoading(false);
        }
      }
    };

    void load();

    return () => {
      alive = false;
    };
  }, [isOpen, activeIdentifier, getEventsByOrganiser]);

  useEffect(() => {
    if (!isOpen) return;

    const onEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };

    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    window.addEventListener("keydown", onEscape);

    return () => {
      document.body.style.overflow = prevOverflow;
      window.removeEventListener("keydown", onEscape);
    };
  }, [isOpen, onClose]);

  const handleIdentifierChange = (identifier: string) => {
    setActiveIdentifier(identifier);
    onOrganiserChange(identifier || null);
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-[110]">
      <button
        type="button"
        onClick={onClose}
        className="absolute inset-0 bg-black/40"
        aria-label="Close organiser history"
      />

      <aside className="absolute right-0 top-0 h-full w-full max-w-2xl overflow-y-auto border-l border-slate-200 bg-white shadow-2xl">
        <div className="sticky top-0 z-10 border-b border-slate-200 bg-white/95 px-6 py-4 backdrop-blur">
          <div className="flex items-start justify-between gap-3">
            <div>
              <p className="text-xs font-semibold uppercase tracking-widest text-slate-400">
                Event Backtracking
              </p>
              <h3 className="mt-1 text-lg font-bold text-slate-900">
                Organiser Event History
              </h3>
              <p className="mt-1 text-sm text-slate-500 break-all">
                {activeIdentifier || "Select an organiser to view event history"}
              </p>
            </div>

            <button
              type="button"
              onClick={onClose}
              className="rounded-lg p-2 text-slate-500 hover:bg-slate-100 hover:text-slate-700"
              aria-label="Close"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        <div className="px-6 py-5">
          <div className="mb-4 rounded-xl border border-slate-200 bg-slate-50 p-3">
            <label htmlFor="organiser-history-select" className="mb-1.5 block text-xs font-semibold uppercase tracking-wider text-slate-500">
              Choose organiser
            </label>
            <select
              id="organiser-history-select"
              value={activeIdentifier}
              onChange={(event) => handleIdentifierChange(event.target.value)}
              className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 outline-none focus:border-[#154CB3] focus:ring-2 focus:ring-[#154CB3]/20"
            >
              <option value="">Select an organiser</option>
              {organiserOptions.map((identifier) => (
                <option key={identifier} value={identifier}>
                  {identifier}
                </option>
              ))}
            </select>
          </div>

          {isLoading ? (
            <div className="flex items-center justify-center rounded-xl border border-slate-200 bg-slate-50 py-16">
              <div className="flex items-center gap-2 text-slate-500">
                <Loader2 className="h-4 w-4 animate-spin" />
                Fetching organiser events...
              </div>
            </div>
          ) : !activeIdentifier ? (
            <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-slate-300 bg-slate-50 py-16 text-center">
              <History className="h-8 w-8 text-slate-300" />
              <p className="mt-3 text-base font-semibold text-slate-700">Choose an organiser</p>
              <p className="mt-1 text-sm text-slate-500">
                Select an organiser from the dropdown to pull up their event history.
              </p>
            </div>
          ) : error ? (
            <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {error}
            </div>
          ) : events.length === 0 ? (
            <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-slate-300 bg-slate-50 py-16 text-center">
              <History className="h-8 w-8 text-slate-300" />
              <p className="mt-3 text-base font-semibold text-slate-700">No events created yet</p>
              <p className="mt-1 text-sm text-slate-500">
                This organiser does not have historical event records yet.
              </p>
            </div>
          ) : (
            <div className="space-y-3">
              {events.map((event) => {
                const status = getEventStatus(event.event_date);
                return (
                  <div
                    key={`${event.event_id}-${event.created_at}`}
                    className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm"
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="min-w-0">
                        <p className="truncate text-sm font-semibold text-slate-900">
                          {event.title || "Untitled Event"}
                        </p>
                        <p className="mt-1 text-xs text-slate-500">
                          Created on {formatDate(event.created_at)}
                        </p>
                      </div>
                      <span
                        className={`inline-flex rounded-full px-2.5 py-1 text-[11px] font-semibold ${status.className}`}
                      >
                        {status.label}
                      </span>
                    </div>

                    <div className="mt-3 flex flex-wrap gap-2">
                      <span className="inline-flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] text-slate-600">
                        <CalendarDays className="h-3 w-3" />
                        {formatDate(event.event_date)}
                      </span>
                      <span className="inline-flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] text-slate-600">
                        <FolderKanban className="h-3 w-3" />
                        {event.fest || "No Fest"}
                      </span>
                      <span className="inline-flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2.5 py-1 text-[11px] text-slate-600">
                        <Building2 className="h-3 w-3" />
                        {event.organizing_dept || "No Department"}
                      </span>
                    </div>

                    <div className="mt-3">
                      <Link
                        href={`/event/${event.event_id}`}
                        className="inline-flex items-center text-xs font-semibold text-[#154CB3] hover:underline"
                      >
                        Open event details
                      </Link>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </aside>
    </div>
  );
}
