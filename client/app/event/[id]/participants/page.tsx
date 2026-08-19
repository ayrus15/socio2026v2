"use client";

import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import { useParams } from "next/navigation";
import { useDebounce } from "@/lib/hooks/useDebounce";
import { useAuth } from "@/context/AuthContext";
import Container from "@/components/Container";
import { User, Users, Plus, Trash2, Sparkles, CheckCircle2 } from "lucide-react";
import {
  addThemedChartsSheet,
  addStructuredTableSheet,
  createThemedWorkbook,
  downloadWorkbook,
  type ThemedSheetColumn,
} from "@/lib/xlsxTheme";

interface CustomField {
  id: string;
  label: string;
  type: string;
  required?: boolean;
}

interface Student {
  id: number;
  registration_id?: string;
  name: string;
  register_number: string;
  course?: string;
  department?: string;
  email: string;
  created_at?: string;
  custom_field_responses?: Record<string, string | number>;
  attendance_status?: string;
}

const ITEMS_PER_PAGE = 20;

export default function StudentsPage() {
  const { session, userData } = useAuth();
  const [students, setStudents] = useState<Student[]>([]);
  const [customFields, setCustomFields] = useState<CustomField[]>([]);
  const [searchQuery, setSearchQuery] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [isDataLoading, setIsDataLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [eventTitle, setEventTitle] = useState<string>("");
  const [attendanceMap, setAttendanceMap] = useState<Record<string, string>>({});
  const [eventOnSpotEnabled, setEventOnSpotEnabled] = useState(false);
  const [eventCreatedBy, setEventCreatedBy] = useState<string[]>([]);
  const [showOnSpotForm, setShowOnSpotForm] = useState(false);

  // Event registration configuration
  const [eventRegType, setEventRegType] = useState<"individual" | "team" | "both">("both");
  const [eventMinParticipants, setEventMinParticipants] = useState<number>(1);
  const [eventMaxParticipants, setEventMaxParticipants] = useState<number>(1);

  // On-spot Mode and Fields
  const [onSpotRegistrationType, setOnSpotRegistrationType] = useState<"individual" | "team">("individual");
  const [onSpotName, setOnSpotName] = useState("");
  const [onSpotRegisterId, setOnSpotRegisterId] = useState("");
  const [onSpotEmail, setOnSpotEmail] = useState("");

  // Team Mode Fields
  const [onSpotTeamName, setOnSpotTeamName] = useState("");
  const [onSpotLeaderName, setOnSpotLeaderName] = useState("");
  const [onSpotLeaderRegisterId, setOnSpotLeaderRegisterId] = useState("");
  const [onSpotLeaderEmail, setOnSpotLeaderEmail] = useState("");
  const [onSpotTeammates, setOnSpotTeammates] = useState<Array<{ id: string; name: string; registerId: string; email: string }>>([]);

  const lookupCacheRef = useRef<Map<string, { name: string; email: string; course?: string; department?: string }>>(new Map());
  const [autoFillStatuses, setAutoFillStatuses] = useState<Record<string, string>>({});

  const [onSpotError, setOnSpotError] = useState<string | null>(null);
  const [onSpotSuccess, setOnSpotSuccess] = useState<string | null>(null);
  const [isOnSpotSubmitting, setIsOnSpotSubmitting] = useState(false);
  const [refreshNonce, setRefreshNonce] = useState(0);

  const params = useParams();
  const event_id = params?.id as string;
  const apiBaseUrl = process.env.NEXT_PUBLIC_API_URL!.replace(/\/api\/?$/, "");

  const currentUserEmail = String(session?.user?.email || "").toLowerCase();
  const isMasterAdmin = Boolean(userData?.is_masteradmin);
  const isOrganiser = Boolean(userData?.is_organiser);
  const isEventOwner =
    Boolean(currentUserEmail) &&
    eventCreatedBy.length > 0 &&
    eventCreatedBy.some(e => e.toLowerCase() === currentUserEmail);
  const canUseOnSpot = eventOnSpotEnabled && (isMasterAdmin || (isOrganiser && isEventOwner));

  // Debounce search for better performance
  const debouncedSearch = useDebounce(searchQuery, 300);

  useEffect(() => {
    window.scrollTo(0, 0);

    const fetchData = async () => {
      if (!event_id) {
        setIsDataLoading(false);
        return;
      }
      setIsDataLoading(true);
      setError(null);
      try {
        // Fetch event details, registrations, and attendance status in parallel
        const [eventResponse, registrationsResponse, attendanceResponse] = await Promise.all([
          fetch(`${apiBaseUrl}/api/events/${event_id}`),
          fetch(`${apiBaseUrl}/api/registrations?event_id=${event_id}`),
          fetch(`${apiBaseUrl}/api/events/${event_id}/participants`)
        ]);
        
        // Parse event custom fields
        if (eventResponse.ok) {
          const eventData = await eventResponse.json();
          const event = eventData.event || eventData;
          setEventTitle(event.title || "");
          setEventOnSpotEnabled(
            event.on_spot === true ||
              event.on_spot === 1 ||
              event.on_spot === "1" ||
              event.on_spot === "true"
          );

          const rawRegType = String(event.registration_type || "both").toLowerCase() as "individual" | "team" | "both";
          const parsedMin = Number(event.min_participants || (rawRegType === "team" ? 2 : 1));
          const parsedMax = Number(event.participants_per_team || event.max_team_size || (rawRegType === "team" ? 10 : 1));

          setEventRegType(rawRegType);
          setEventMinParticipants(parsedMin);
          setEventMaxParticipants(parsedMax);

          const isIndivOnly = rawRegType === "individual" || (parsedMax <= 1 && parsedMin <= 1);
          const isTeamOnly = rawRegType === "team" || parsedMin > 1;

          if (isIndivOnly) {
            setOnSpotRegistrationType("individual");
          } else if (isTeamOnly) {
            setOnSpotRegistrationType("team");
            const requiredTeammates = Math.max(0, parsedMin - 1);
            setOnSpotTeammates((prev) => {
              if (prev.length >= requiredTeammates) return prev;
              const needed = requiredTeammates - prev.length;
              return [
                ...prev,
                ...Array.from({ length: needed }).map((_, i) => ({
                  id: String(Date.now() + i),
                  name: "",
                  registerId: "",
                  email: "",
                })),
              ];
            });
          }
          const rawCb = event.created_by;
          const creatorEmails: string[] = Array.isArray(rawCb)
            ? rawCb.filter((e: unknown) => typeof e === "string" && e)
            : typeof rawCb === "object" && rawCb !== null
              ? [rawCb.event_creator, rawCb.fest_creator].filter(Boolean)
              : typeof rawCb === "string" && rawCb ? [rawCb] : [];
          setEventCreatedBy(creatorEmails);
          let fields: CustomField[] = [];
          if (event.custom_fields) {
            if (typeof event.custom_fields === 'string') {
              try {
                fields = JSON.parse(event.custom_fields);
              } catch (e) {
                console.warn('Failed to parse custom_fields:', e);
              }
            } else if (Array.isArray(event.custom_fields)) {
              fields = event.custom_fields;
            }
          }
          setCustomFields(fields);
        }
        
        // Build attendance map by registration_id
        if (attendanceResponse.ok) {
          const attendanceData = await attendanceResponse.json();
          const map: Record<string, string> = {};
          (attendanceData.participants || []).forEach((p: any) => {
            const key = String(p.registration_id || p.id || "");
            if (key) {
              map[key] = p.attendance_status || "absent";
            }
          });
          setAttendanceMap(map);
        }

        if (!registrationsResponse.ok) {
          let errorMessage = `Error: ${registrationsResponse.status} ${registrationsResponse.statusText}`;
          try {
            const errorData = await registrationsResponse.json();
            errorMessage = `Server Error: ${
              errorData.details || errorData.error || "Unknown error"
            }`;
          } catch {
            const errorText = await registrationsResponse.text();
            errorMessage = `Error ${
              registrationsResponse.status
            }: Failed to retrieve data. ${errorText.substring(0, 150)}`;
          }
          throw new Error(errorMessage);
        }
        const data = await registrationsResponse.json();
        const mappedStudents = (data.registrations || []).map((reg: any) => ({
          id: reg.registration_id || reg.id || 0,
          registration_id: reg.registration_id || reg.id || "",
          name: reg.registration_type === 'individual' ? reg.individual_name : reg.team_leader_name || "",
          register_number: reg.registration_type === 'individual' ? reg.individual_register_number : reg.team_leader_register_number || "",
          course: reg.course || "",
          department: reg.department || "",
          email: reg.registration_type === 'individual' ? reg.individual_email : reg.team_leader_email || "",
          created_at: reg.created_at || "",
          custom_field_responses: reg.custom_field_responses || {},
          attendance_status: reg.attendance_status || "",
        }));
        setStudents(mappedStudents);
      } catch (err) {
        console.error("Fetch error:", err);
        setError(
          err instanceof Error ? err.message : "Failed to load participants."
        );
      } finally {
        setIsDataLoading(false);
      }
    };
    if (event_id) fetchData();
  }, [event_id, apiBaseUrl, refreshNonce]);

  const lookupParticipant = async (
    identifier: string,
    targetFieldId: string,
    onFound: (data: { name: string; email: string }) => void
  ) => {
    const trimmedId = identifier.trim();
    if (!trimmedId || trimmedId.length < 3) return;

    const normalizedKey = trimmedId.toUpperCase();

    // 1. Check client LRU cache first (0 network calls)
    if (lookupCacheRef.current.has(normalizedKey)) {
      const cached = lookupCacheRef.current.get(normalizedKey)!;
      onFound({ name: cached.name, email: cached.email });
      setAutoFillStatuses((prev) => ({
        ...prev,
        [targetFieldId]: `✨ Loaded from profile: ${cached.name}${cached.email ? ` (${cached.email})` : ''}`,
      }));
      return;
    }

    if (!session?.access_token) return;

    try {
      setAutoFillStatuses((prev) => ({ ...prev, [targetFieldId]: "Fetching profile..." }));
      const res = await fetch(`${apiBaseUrl}/api/users/lookup?identifier=${encodeURIComponent(trimmedId)}`, {
        headers: {
          Authorization: `Bearer ${session.access_token}`,
        },
      });

      if (res.ok) {
        const data = await res.json();
        if (data.found && data.user) {
          const fetchedUser = {
            name: data.user.name || "",
            email: data.user.email || "",
            course: data.user.course,
            department: data.user.department,
          };
          lookupCacheRef.current.set(normalizedKey, fetchedUser);
          onFound({ name: fetchedUser.name, email: fetchedUser.email });
          setAutoFillStatuses((prev) => ({
            ...prev,
            [targetFieldId]: `✨ Auto-filled: ${fetchedUser.name}${fetchedUser.email ? ` (${fetchedUser.email})` : ''}`,
          }));
          return;
        }
      }
      setAutoFillStatuses((prev) => ({ ...prev, [targetFieldId]: "" }));
    } catch (_err) {
      setAutoFillStatuses((prev) => ({ ...prev, [targetFieldId]: "" }));
    }
  };

  const handleOnSpotRegistration = async () => {
    setOnSpotError(null);
    setOnSpotSuccess(null);

    if (!session?.access_token) {
      setOnSpotError("Please sign in again to add on-spot participants.");
      return;
    }

    setIsOnSpotSubmitting(true);
    try {
      let bodyData: any = {};

      if (onSpotRegistrationType === "individual") {
        const attendeeName = onSpotName.trim();
        const registerIdentifier = onSpotRegisterId.trim();
        const attendeeEmail = onSpotEmail.trim();

        if (!attendeeName) {
          setOnSpotError("Participant name is required.");
          setIsOnSpotSubmitting(false);
          return;
        }
        if (!registerIdentifier) {
          setOnSpotError("Register number or visitor ID is required.");
          setIsOnSpotSubmitting(false);
          return;
        }

        bodyData = {
          registration_type: "individual",
          name: attendeeName,
          register_number: registerIdentifier,
          email: attendeeEmail || undefined,
        };
      } else {
        // Team registration
        const teamName = onSpotTeamName.trim();
        const leaderName = onSpotLeaderName.trim();
        const leaderRegisterId = onSpotLeaderRegisterId.trim();
        const leaderEmail = onSpotLeaderEmail.trim();

        if (!teamName) {
          setOnSpotError("Team name is required.");
          setIsOnSpotSubmitting(false);
          return;
        }
        if (!leaderName) {
          setOnSpotError("Team leader name is required.");
          setIsOnSpotSubmitting(false);
          return;
        }
        if (!leaderRegisterId) {
          setOnSpotError("Team leader register number or visitor ID is required.");
          setIsOnSpotSubmitting(false);
          return;
        }

        const validTeammates = onSpotTeammates
          .map((tm) => ({
            name: tm.name.trim(),
            register_number: tm.registerId.trim(),
            email: tm.email.trim() || undefined,
          }))
          .filter((tm) => tm.name && tm.register_number);

        bodyData = {
          registration_type: "team",
          team_name: teamName,
          team_leader: {
            name: leaderName,
            register_number: leaderRegisterId,
            email: leaderEmail || undefined,
          },
          teammates: validTeammates,
        };
      }

      const response = await fetch(`${apiBaseUrl}/api/events/${event_id}/on-spot-register`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${session.access_token}`,
        },
        body: JSON.stringify(bodyData),
      });

      const payload = await response.json().catch(() => null);
      if (!response.ok) {
        throw new Error(payload?.error || `Failed with status ${response.status}`);
      }

      setOnSpotSuccess(
        onSpotRegistrationType === "team"
          ? "On-spot team registration added successfully!"
          : "On-spot participant added successfully."
      );
      setOnSpotName("");
      setOnSpotRegisterId("");
      setOnSpotEmail("");
      setOnSpotTeamName("");
      setOnSpotLeaderName("");
      setOnSpotLeaderRegisterId("");
      setOnSpotLeaderEmail("");
      setOnSpotTeammates([]);
      setAutoFillStatuses({});
      setRefreshNonce((prev) => prev + 1);
    } catch (submitError: any) {
      setOnSpotError(submitError?.message || "Unable to complete on-spot registration.");
    } finally {
      setIsOnSpotSubmitting(false);
    }
  };

  const handleGenerateExcel = async () => {
    if (students.length === 0) {
      console.log("No participant data to export.");
      return;
    }

    const workbook = createThemedWorkbook("SOCIO - Christ University");

    const customFieldColumns = customFields.map((field) => ({
      field,
      key: `custom_${field.id}`,
    }));

    type ParticipantExportRow = Record<string, string | number | null | undefined>;

    const columns: Array<ThemedSheetColumn<ParticipantExportRow>> = [
      { header: "Name", key: "name", width: 25 },
      { header: "Register No.", key: "register_number", width: 16, horizontal: "center" },
      { header: "Course", key: "course", width: 20 },
      { header: "Department", key: "department", width: 20 },
      { header: "E-mail", key: "email", width: 35, kind: "email" },
      ...customFieldColumns.map(({ field, key }) => ({
        header: field.label,
        key,
        width: 30,
        kind: (field.type === "url" ? "link" : "text") as "link" | "text",
      })),
      { header: "Attendance", key: "attendance", width: 14, kind: "status" },
    ];

    const rows: ParticipantExportRow[] = students.map((student) => {
      const row: ParticipantExportRow = {
        name: student.name || "",
        register_number: student.register_number || "",
        course: student.course || "",
        department: student.department || "",
        email: student.email || "",
      };

      customFieldColumns.forEach(({ field, key }) => {
        const value = student.custom_field_responses?.[field.id];
        row[key] = value !== undefined && value !== null ? String(value) : "";
      });

      const attendanceKey = String(student.registration_id || student.id || "");
      row.attendance = attendanceMap[attendanceKey] || student.attendance_status || "absent";

      return row;
    });

    addStructuredTableSheet(workbook, {
      sheetName: "Participants",
      columns,
      rows,
      rowHeight: 24,
    });

    const attendanceChartData = [
      {
        label: "Attended",
        value: rows.filter((row) => String(row.attendance ?? "").toLowerCase() === "attended").length,
      },
      {
        label: "Absent",
        value: rows.filter((row) => String(row.attendance ?? "").toLowerCase() === "absent").length,
      },
      {
        label: "Pending",
        value: rows.filter((row) => String(row.attendance ?? "").toLowerCase() === "pending").length,
      },
      {
        label: "Unmarked",
        value: rows.filter((row) => String(row.attendance ?? "").toLowerCase() === "unmarked").length,
      },
    ];

    const departmentChartData = Object.entries(
      rows.reduce<Record<string, number>>((acc, row) => {
        const dept = String(row.department || "Unknown");
        acc[dept] = (acc[dept] || 0) + 1;
        return acc;
      }, {})
    )
      .map(([label, value]) => ({ label, value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 10);

    addThemedChartsSheet(workbook, {
      title: "Participants Visual Overview",
      subtitle: "Chart snapshots are embedded for quick review.",
      primaryChart: {
        title: "Participants by Department",
        type: "bar",
        data: departmentChartData,
      },
      secondaryChart: {
        title: "Attendance Status Mix",
        type: "donut",
        data: attendanceChartData,
      },
    });

    await downloadWorkbook(workbook, `participants-${event_id}.xlsx`);
    console.log("Excel file generated.");
  };

  const filteredStudents = students.filter((student) => {
    const searchLower = debouncedSearch.toLowerCase();
    const nameMatch = student.name?.toLowerCase().includes(searchLower);
    const registerNumberString =
      student.register_number != null ? String(student.register_number) : "";
    const registerNumberMatch = registerNumberString
      .toLowerCase()
      .includes(searchLower);
    const emailMatch = student.email?.toLowerCase().includes(searchLower);
    return nameMatch || registerNumberMatch || emailMatch;
  });

  // Pagination
  const totalPages = Math.ceil(filteredStudents.length / ITEMS_PER_PAGE);
  const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
  const endIndex = startIndex + ITEMS_PER_PAGE;
  const paginatedStudents = filteredStudents.slice(startIndex, endIndex);

  // Reset to page 1 when search changes
  useEffect(() => {
    setCurrentPage(1);
  }, [debouncedSearch]);

  if (params && !event_id && isDataLoading) {
    return (
      <div className="min-h-screen flex flex-col bg-white">
        <div className="flex-1 flex justify-center items-center h-64">
          <span className="text-gray-600">Loading event data...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen flex flex-col bg-white">
      <Container>
      <div className="pt-6 md:pt-8">
        <div className="mb-4">
          <Link
            href="/manage"
            className="inline-flex items-center text-[#154CB3] hover:text-[#063168] font-medium transition-colors"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth={2}
              stroke="currentColor"
              className="w-4 h-4 mr-1"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M10.5 19.5 3 12m0 0 7.5-7.5M3 12h18"
              />
            </svg>
            Back to Dashboard
          </Link>
        </div>
      </div>

      <main className="flex-1 pb-6 md:pb-8">
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 md:mb-8 gap-4 sm:gap-0">
          <h1 className="text-xl md:text-2xl font-bold text-[#154CB3]">
            Participants ({students.length})
          </h1>
          <div className="flex flex-wrap items-center gap-3">
            {canUseOnSpot && (
              <button
                onClick={() => {
                  setShowOnSpotForm((prev) => !prev);
                  setOnSpotError(null);
                  setOnSpotSuccess(null);
                }}
                className="bg-white text-[#154CB3] border border-[#154CB3] text-sm px-4 py-2 rounded-full font-medium hover:bg-[#154CB3] hover:text-white transition-colors"
              >
                {showOnSpotForm ? "Hide on-spot" : "On Spot Registration"}
              </button>
            )}
            <Link
              href={`/attendance?eventId=${event_id}&eventTitle=${encodeURIComponent(eventTitle || "Event")}`}
              className="bg-white text-[#154CB3] border border-[#154CB3] text-sm px-4 py-2 rounded-full font-medium hover:bg-[#154CB3] hover:text-white transition-colors"
            >
              Mark attendance
            </Link>
            <button
              onClick={handleGenerateExcel}
              className="bg-[#154CB3] cursor-pointer text-white text-sm px-4 py-2 rounded-full font-medium hover:bg-[#063168] transition-colors focus:outline-none focus:ring-2 focus:ring-[#154CB3] focus:ring-opacity-50 disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={isDataLoading || students.length === 0}
            >
              Generate excel sheet
            </button>
          </div>
        </div>

        {showOnSpotForm && canUseOnSpot && (() => {
          const isIndivOnly = eventRegType === "individual" || (eventMaxParticipants <= 1 && eventMinParticipants <= 1);
          const isTeamOnly = eventRegType === "team" || eventMinParticipants > 1;
          const requiredTeammatesCount = Math.max(0, eventMinParticipants - 1);
          const totalTeamMembers = 1 + onSpotTeammates.length;
          const canAddMoreTeammates = totalTeamMembers < eventMaxParticipants;

          const handleSwitchToTeamMode = () => {
            setOnSpotRegistrationType("team");
            setOnSpotError(null);
            setOnSpotSuccess(null);
            if (onSpotTeammates.length < requiredTeammatesCount) {
              const needed = requiredTeammatesCount - onSpotTeammates.length;
              const newItems = Array.from({ length: needed }).map((_, i) => ({
                id: String(Date.now() + i + Math.random()),
                name: "",
                registerId: "",
                email: "",
              }));
              setOnSpotTeammates((prev) => [...prev, ...newItems]);
            }
          };

          const activeMode = isIndivOnly ? "individual" : isTeamOnly ? "team" : onSpotRegistrationType;

          return (
            <div className="mb-6 border border-blue-200 bg-blue-50/80 rounded-xl p-5 shadow-xs">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4 border-b border-blue-100 pb-3">
                <div>
                  <h2 className="text-base font-bold text-[#063168]">
                    On-Spot Registration Desk {isIndivOnly ? "(Individual)" : isTeamOnly ? "(Team Registration)" : ""}
                  </h2>
                  <p className="text-xs text-slate-500">
                    Enter Register No. or Visitor ID (VID) to automatically pre-fill student and visitor details.
                  </p>
                </div>

                {/* Mode Toggle Switch (only if both modes allowed) */}
                {!isIndivOnly && !isTeamOnly && (
                  <div className="inline-flex p-1 bg-white border border-blue-200 rounded-lg">
                    <button
                      type="button"
                      onClick={() => {
                        setOnSpotRegistrationType("individual");
                        setOnSpotError(null);
                        setOnSpotSuccess(null);
                      }}
                      className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-all ${
                        activeMode === "individual"
                          ? "bg-[#154CB3] text-white shadow-xs"
                          : "text-slate-600 hover:text-slate-900"
                      }`}
                    >
                      <User className="w-3.5 h-3.5" />
                      Individual
                    </button>
                    <button
                      type="button"
                      onClick={handleSwitchToTeamMode}
                      className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-all ${
                        activeMode === "team"
                          ? "bg-[#154CB3] text-white shadow-xs"
                          : "text-slate-600 hover:text-slate-900"
                      }`}
                    >
                      <Users className="w-3.5 h-3.5" />
                      Team Registration
                    </button>
                  </div>
                )}
              </div>

              {activeMode === "individual" ? (
                /* INDIVIDUAL FORM */
                <div className="space-y-3">
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                    <div>
                      <label className="block text-xs font-medium text-slate-700 mb-1">
                        Register No. or Visitor ID (VID) *
                      </label>
                      <input
                        type="text"
                        value={onSpotRegisterId}
                        onChange={(e) => {
                          const val = e.target.value;
                          setOnSpotRegisterId(val);
                          if (val.trim().length >= 5) {
                            lookupParticipant(val, "indiv_reg", ({ name, email }) => {
                              if (name) setOnSpotName(name);
                              if (email) setOnSpotEmail(email);
                            });
                          }
                        }}
                        onBlur={() => {
                          lookupParticipant(onSpotRegisterId, "indiv_reg", ({ name, email }) => {
                            if (name) setOnSpotName(name);
                            if (email) setOnSpotEmail(email);
                          });
                        }}
                        placeholder="e.g. 2230101 or VIS-1001"
                        className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                      />
                      {autoFillStatuses["indiv_reg"] && (
                        <p className="text-[11px] font-medium text-emerald-700 mt-1 flex items-center gap-1">
                          <Sparkles className="w-3 h-3 text-amber-500" />
                          {autoFillStatuses["indiv_reg"]}
                        </p>
                      )}
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-slate-700 mb-1">Participant Name *</label>
                      <input
                        type="text"
                        value={onSpotName}
                        onChange={(e) => setOnSpotName(e.target.value)}
                        placeholder="Participant Full Name"
                        className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                      />
                    </div>

                    <div>
                      <label className="block text-xs font-medium text-slate-700 mb-1">Email Address</label>
                      <input
                        type="email"
                        value={onSpotEmail}
                        onChange={(e) => setOnSpotEmail(e.target.value)}
                        placeholder="email@example.com (optional)"
                        className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                      />
                    </div>
                  </div>
                </div>
              ) : (
                /* TEAM FORM */
                <div className="space-y-4">
                  {/* Team Rules Guidance Banner */}
                  <div className="bg-blue-100/70 border border-blue-200 rounded-lg p-2.5 flex items-center justify-between text-xs text-[#063168]">
                    <div className="flex items-center gap-2 font-semibold">
                      <Users className="w-4 h-4 text-[#154CB3]" />
                      <span>
                        Team Rules: Min {eventMinParticipants} to Max {eventMaxParticipants} members per team
                      </span>
                    </div>
                    <span className="text-[11px] font-bold bg-white text-[#154CB3] px-2.5 py-0.5 rounded-full border border-blue-200">
                      Total: {totalTeamMembers} / {eventMaxParticipants} members
                    </span>
                  </div>

                  <div>
                    <label className="block text-xs font-bold text-slate-800 mb-1">Team Name (Team ID) *</label>
                    <input
                      type="text"
                      value={onSpotTeamName}
                      onChange={(e) => setOnSpotTeamName(e.target.value)}
                      placeholder="e.g. Alpha Squad, Code Warriors..."
                      className="w-full max-w-md rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm font-semibold text-[#063168] focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                    />
                    <p className="text-[11px] text-slate-500 mt-0.5">
                      Team Name will be used as the unique team identifier for this registration.
                    </p>
                  </div>

                  {/* Team Leader Section */}
                  <div className="border border-blue-200 bg-white rounded-lg p-3">
                    <div className="flex items-center gap-2 mb-2">
                      <span className="bg-blue-100 text-[#154CB3] text-[10px] font-extrabold uppercase px-2 py-0.5 rounded-full">
                        Team Leader (Member #1 - Required)
                      </span>
                    </div>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                      <div>
                        <label className="block text-xs font-medium text-slate-700 mb-1">Leader Reg No. / VID *</label>
                        <input
                          type="text"
                          value={onSpotLeaderRegisterId}
                          onChange={(e) => {
                            const val = e.target.value;
                            setOnSpotLeaderRegisterId(val);
                            if (val.trim().length >= 5) {
                              lookupParticipant(val, "leader_reg", ({ name, email }) => {
                                if (name) setOnSpotLeaderName(name);
                                if (email) setOnSpotLeaderEmail(email);
                              });
                            }
                          }}
                          onBlur={() => {
                            lookupParticipant(onSpotLeaderRegisterId, "leader_reg", ({ name, email }) => {
                              if (name) setOnSpotLeaderName(name);
                              if (email) setOnSpotLeaderEmail(email);
                            });
                          }}
                          placeholder="e.g. 2230101 or VIS-1001"
                          className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                        />
                        {autoFillStatuses["leader_reg"] && (
                          <p className="text-[11px] font-medium text-emerald-700 mt-1 flex items-center gap-1">
                            <Sparkles className="w-3 h-3 text-amber-500" />
                            {autoFillStatuses["leader_reg"]}
                          </p>
                        )}
                      </div>

                      <div>
                        <label className="block text-xs font-medium text-slate-700 mb-1">Leader Name *</label>
                        <input
                          type="text"
                          value={onSpotLeaderName}
                          onChange={(e) => setOnSpotLeaderName(e.target.value)}
                          placeholder="Leader Full Name"
                          className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                        />
                      </div>

                      <div>
                        <label className="block text-xs font-medium text-slate-700 mb-1">Leader Email</label>
                        <input
                          type="email"
                          value={onSpotLeaderEmail}
                          onChange={(e) => setOnSpotLeaderEmail(e.target.value)}
                          placeholder="email@example.com (optional)"
                          className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                        />
                      </div>
                    </div>
                  </div>

                  {/* Teammates Section */}
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <label className="block text-xs font-bold text-slate-800">
                        Teammates ({onSpotTeammates.length})
                      </label>
                      <button
                        type="button"
                        disabled={!canAddMoreTeammates}
                        onClick={() => {
                          if (canAddMoreTeammates) {
                            setOnSpotTeammates((prev) => [
                              ...prev,
                              { id: String(Date.now() + Math.random()), name: "", registerId: "", email: "" },
                            ]);
                          }
                        }}
                        className="inline-flex items-center gap-1 text-xs font-semibold text-[#154CB3] hover:text-[#063168] bg-white border border-blue-300 px-2.5 py-1 rounded-md shadow-xs hover:bg-blue-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                      >
                        <Plus className="w-3.5 h-3.5" />
                        {canAddMoreTeammates ? "Add Teammate" : `Max (${eventMaxParticipants}) Reached`}
                      </button>
                    </div>

                    {onSpotTeammates.length === 0 ? (
                      <p className="text-xs text-slate-500 italic bg-white/60 p-3 rounded-lg border border-dashed border-blue-200">
                        No teammates added yet. Click "+ Add Teammate" to include members to this team.
                      </p>
                    ) : (
                      onSpotTeammates.map((tm, index) => {
                        const isRequiredSlot = index < requiredTeammatesCount;
                        return (
                          <div key={tm.id} className="border border-slate-200 bg-white rounded-lg p-3 relative">
                            <div className="flex items-center justify-between mb-2">
                              <div className="flex items-center gap-2">
                                <span className="text-[11px] font-bold text-slate-700">Teammate #{index + 1}</span>
                                {isRequiredSlot ? (
                                  <span className="bg-red-50 text-red-700 border border-red-200 text-[10px] font-extrabold uppercase px-2 py-0.5 rounded-full">
                                    Required
                                  </span>
                                ) : (
                                  <span className="bg-slate-100 text-slate-600 border border-slate-200 text-[10px] font-semibold px-2 py-0.5 rounded-full">
                                    Optional
                                  </span>
                                )}
                              </div>

                              {!isRequiredSlot && (
                                <button
                                  type="button"
                                  onClick={() => {
                                    setOnSpotTeammates((prev) => prev.filter((t) => t.id !== tm.id));
                                  }}
                                  className="text-slate-400 hover:text-red-600 transition-colors p-1"
                                  title="Remove Teammate"
                                >
                                  <Trash2 className="w-3.5 h-3.5" />
                                </button>
                              )}
                            </div>

                            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                              <div>
                                <input
                                  type="text"
                                  value={tm.registerId}
                                  onChange={(e) => {
                                    const val = e.target.value;
                                    setOnSpotTeammates((prev) =>
                                      prev.map((item) => (item.id === tm.id ? { ...item, registerId: val } : item))
                                    );
                                    if (val.trim().length >= 5) {
                                      lookupParticipant(val, `tm_${tm.id}`, ({ name, email }) => {
                                        setOnSpotTeammates((prev) =>
                                          prev.map((item) =>
                                            item.id === tm.id
                                              ? {
                                                  ...item,
                                                  name: name || item.name,
                                                  email: email || item.email,
                                                }
                                              : item
                                          )
                                        );
                                      });
                                    }
                                  }}
                                  onBlur={() => {
                                    lookupParticipant(tm.registerId, `tm_${tm.id}`, ({ name, email }) => {
                                      setOnSpotTeammates((prev) =>
                                        prev.map((item) =>
                                          item.id === tm.id
                                            ? {
                                                ...item,
                                                name: name || item.name,
                                                email: email || item.email,
                                              }
                                            : item
                                        )
                                      );
                                    });
                                  }}
                                  placeholder={isRequiredSlot ? "Reg No. or Visitor ID *" : "Reg No. or Visitor ID"}
                                  className="w-full rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                                />
                                {autoFillStatuses[`tm_${tm.id}`] && (
                                  <p className="text-[10px] font-medium text-emerald-700 mt-1 flex items-center gap-1">
                                    <Sparkles className="w-3 h-3 text-amber-500" />
                                    {autoFillStatuses[`tm_${tm.id}`]}
                                  </p>
                                )}
                              </div>

                              <div>
                                <input
                                  type="text"
                                  value={tm.name}
                                  onChange={(e) => {
                                    const val = e.target.value;
                                    setOnSpotTeammates((prev) =>
                                      prev.map((item) => (item.id === tm.id ? { ...item, name: val } : item))
                                    );
                                  }}
                                  placeholder={isRequiredSlot ? "Full Name *" : "Full Name"}
                                  className="w-full rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                                />
                              </div>

                              <div>
                                <input
                                  type="email"
                                  value={tm.email}
                                  onChange={(e) => {
                                    const val = e.target.value;
                                    setOnSpotTeammates((prev) =>
                                      prev.map((item) => (item.id === tm.id ? { ...item, email: val } : item))
                                    );
                                  }}
                                  placeholder="Email (optional)"
                                  className="w-full rounded-lg border border-gray-300 bg-white px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-[#154CB3]"
                                />
                              </div>
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                </div>
              )}

              {onSpotError && (
                <div className="mt-3 p-2.5 rounded-lg bg-red-100 border border-red-200 text-xs font-semibold text-red-700">
                  {onSpotError}
                </div>
              )}
              {onSpotSuccess && (
                <div className="mt-3 p-2.5 rounded-lg bg-emerald-100 border border-emerald-200 text-xs font-semibold text-emerald-800 flex items-center gap-1.5">
                  <CheckCircle2 className="w-4 h-4 text-emerald-600" />
                  {onSpotSuccess}
                </div>
              )}

              <div className="mt-4 flex items-center justify-end border-t border-blue-100 pt-3">
                <button
                  onClick={handleOnSpotRegistration}
                  disabled={isOnSpotSubmitting}
                  className="bg-[#154CB3] text-white text-sm px-5 py-2.5 rounded-lg font-semibold hover:bg-[#063168] transition-colors disabled:opacity-60 disabled:cursor-not-allowed shadow-xs"
                >
                  {isOnSpotSubmitting
                    ? "Processing..."
                    : activeMode === "team"
                    ? "Add Team Registration"
                    : "Add Participant"}
                </button>
              </div>
            </div>
          );
        })()}

        <div className="relative mb-6 md:mb-8">
          <div className="absolute inset-y-0 left-4 flex items-center pointer-events-none">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth={1.5}
              stroke="currentColor"
              className="w-5 h-5 text-gray-400"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="m21 21-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z"
              />
            </svg>
          </div>
          <input
            type="text"
            placeholder="Search student by name, register no, or email..."
            className="w-full pl-12 pr-4 py-3 bg-gray-100 rounded-full border border-gray-200 focus:outline-none focus:ring-2 focus:ring-[#154CB3] focus:border-transparent transition-all"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>

        {/* Desktop Table - Single scrollable container for header and rows */}
        <div className="hidden md:block overflow-x-auto">
          {/* Table Header */}
          <div className={`grid gap-4 px-4 py-4 text-gray-500 font-medium border-b border-gray-200`}
               style={{ gridTemplateColumns: `200px 120px 150px 150px 250px ${customFields.map(() => '180px').join(' ')}`.trim(), minWidth: 'max-content' }}>
            <div>Name</div>
            <div>Register No.</div>
            <div>Course</div>
            <div>Department</div>
            <div>E-mail</div>
            {customFields.map(field => (
              <div key={field.id} className="text-[#154CB3] font-semibold">
                {field.label}
              </div>
            ))}
          </div>
          
          {/* Table Body */}
          {!isDataLoading && !error && paginatedStudents.length > 0 && paginatedStudents.map((student: Student) => (
            <div 
              key={student.id}
              className="grid gap-4 px-4 py-4 items-center border-b border-gray-200 hover:bg-gray-50 transition-colors"
              style={{ gridTemplateColumns: `200px 120px 150px 150px 250px ${customFields.map(() => '180px').join(' ')}`.trim(), minWidth: 'max-content' }}
            >
              <div className="font-medium truncate">
                {student.name || "N/A"}
              </div>
              <div>{student.register_number || "N/A"}</div>
              <div>{student.course || "N/A"}</div>
              <div>{student.department || "N/A"}</div>
              <div className="text-[#154CB3] truncate">
                {student.email || "N/A"}
              </div>
              {customFields.map(field => {
                const value = student.custom_field_responses?.[field.id];
                return (
                  <div key={field.id} className="truncate">
                    {field.type === 'url' && value ? (
                      <a 
                        href={String(value)} 
                        target="_blank" 
                        rel="noopener noreferrer"
                        className="text-[#154CB3] hover:underline"
                      >
                        {String(value)}
                      </a>
                    ) : (
                      <span>{value !== undefined && value !== null ? String(value) : "N/A"}</span>
                    )}
                  </div>
                );
              })}
            </div>
          ))}
        </div>

        {isDataLoading ? (
          <div className="flex justify-center items-center h-64">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
              strokeWidth={2}
              stroke="currentColor"
              className="size-8 animate-spin text-[#063168]"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M16.023 9.348h4.992v-.001M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182m0-4.991v4.99"
              />
            </svg>
            <span className="ml-2 text-gray-600">Loading participants...</span>
          </div>
        ) : error ? (
          <div className="flex flex-col justify-center items-center h-64 text-red-500">
            <p className="font-semibold">Failed to load data</p>
            <p className="text-sm mt-1">{error}</p>
          </div>
        ) : (
          <>
            {/* Mobile Card Views */}
            <div className="md:hidden">
              {paginatedStudents.length > 0 ? (
                paginatedStudents.map((student: Student) => (
                  <div
                    key={student.id}
                    className="mb-4 border rounded-lg shadow-sm border-gray-200"
                  >
                    <div className="p-4">
                      <div className="font-medium text-lg mb-2">
                        {student.name || "N/A"}
                      </div>
                      <div className="grid grid-cols-1 gap-2">
                        <div className="flex">
                          <span className="text-gray-500 w-28 flex-shrink-0">
                            Register No.
                          </span>
                          <span>{student.register_number || "N/A"}</span>
                        </div>
                        <div className="flex">
                          <span className="text-gray-500 w-28 flex-shrink-0">
                            Course
                          </span>
                          <span>{student.course || "N/A"}</span>
                        </div>
                        <div className="flex">
                          <span className="text-gray-500 w-28 flex-shrink-0">
                            Department
                          </span>
                          <span>{student.department || "N/A"}</span>
                        </div>
                        <div className="flex">
                          <span className="text-gray-500 w-28 flex-shrink-0">
                            E-mail
                          </span>
                          <span className="text-[#154CB3] break-all">
                            {student.email || "N/A"}
                          </span>
                        </div>
                        {/* Custom Fields in Mobile View */}
                        {customFields.length > 0 && (
                          <div className="mt-3 pt-3 border-t border-gray-200">
                            <div className="text-sm font-medium text-[#154CB3] mb-2">Additional Information</div>
                            {customFields.map(field => {
                              const value = student.custom_field_responses?.[field.id];
                              return (
                                <div key={field.id} className="flex flex-col mb-2">
                                  <span className="text-gray-500 text-xs">{field.label}</span>
                                  {field.type === 'url' && value ? (
                                    <a 
                                      href={String(value)} 
                                      target="_blank" 
                                      rel="noopener noreferrer"
                                      className="text-[#154CB3] text-sm break-all hover:underline"
                                    >
                                      {String(value)}
                                    </a>
                                  ) : (
                                    <span className="text-sm break-all">{value !== undefined && value !== null ? String(value) : "N/A"}</span>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="flex justify-center items-center h-32 text-gray-500">
                  {searchQuery
                    ? "No participants found matching your search criteria."
                    : "No participants registered for this event yet."}
                </div>
              )}
            </div>

            {/* Desktop - Empty state only */}
            {paginatedStudents.length === 0 && (
              <div className="hidden md:flex justify-center items-center h-32 text-gray-500">
                {searchQuery
                  ? "No participants found matching your search criteria."
                  : "No participants registered for this event yet."}
              </div>
            )}

            {/* Pagination Controls */}
            {filteredStudents.length > ITEMS_PER_PAGE && (
              <div className="flex justify-center items-center gap-4 py-8 border-t mt-6">
                <button
                  onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                  disabled={currentPage === 1}
                  className="px-4 py-2 bg-[#154CB3] text-white rounded-lg disabled:bg-gray-300 disabled:cursor-not-allowed hover:bg-[#154cb3eb] transition-colors font-medium"
                >
                  Previous
                </button>
                <span className="text-gray-700 font-medium">
                  Page {currentPage} of {totalPages} ({filteredStudents.length} total)
                </span>
                <button
                  onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                  disabled={currentPage === totalPages}
                  className="px-4 py-2 bg-[#154CB3] text-white rounded-lg disabled:bg-gray-300 disabled:cursor-not-allowed hover:bg-[#154cb3eb] transition-colors font-medium"
                >
                  Next
                </button>
              </div>
            )}
          </>
        )}
      </main>
      </Container>
    </div>
  );
}
