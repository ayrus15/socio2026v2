"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useEffect, useMemo, useRef, useState } from "react";
import {
  EventPreviewData,
  getEventPreviewDraft,
} from "@/app/lib/eventPreviewDraft";

const toTitleCase = (value: string): string =>
  value
    .split(" ")
    .map((chunk) =>
      chunk.length > 0
        ? `${chunk[0].toUpperCase()}${chunk.slice(1).toLowerCase()}`
        : chunk
    )
    .join(" ");

const ClockIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="size-5 text-[#063168] flex-shrink-0"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z"
    />
  </svg>
);

const CalendarIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="size-5 text-[#063168] flex-shrink-0"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M6.75 3v2.25M17.25 3v2.25M3 18.75V7.5a2.25 2.25 0 012.25-2.25h13.5A2.25 2.25 0 0121 7.5v11.25m-18 0A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V10.5"
    />
  </svg>
);

const LocationIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="size-5 text-[#063168] flex-shrink-0"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M15 10.5a3 3 0 11-6 0 3 3 0 016 0Z"
    />
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M19.5 10.5c0 7.142-7.5 11.25-7.5 11.25S4.5 17.642 4.5 10.5a7.5 7.5 0 1115 0Z"
    />
  </svg>
);

const TicketIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="size-5 text-[#063168] flex-shrink-0"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M16.5 6v.75m0 3v.75m0 3v.75m0 3V18m-9-5.25h5.25M7.5 15h3M3.375 5.25c-.621 0-1.125.504-1.125 1.125v3.026a2.999 2.999 0 010 5.198v3.026c0 .621.504 1.125 1.125 1.125h17.25c.621 0 1.125-.504 1.125-1.125v-3.026a2.999 2.999 0 010-5.198V6.375c0-.621-.504-1.125-1.125-1.125H3.375Z"
    />
  </svg>
);

const UsersIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="size-5 text-[#063168] flex-shrink-0"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M18 18.72a9.094 9.094 0 003.741-.479 3 3 0 00-4.682-2.72m.94 3.198.001.031c0 .225-.012.447-.037.666A11.944 11.944 0 0112 21c-2.17 0-4.207-.576-5.963-1.584A6.062 6.062 0 016 18.719m12 0a5.971 5.971 0 00-.941-3.197m0 0A5.995 5.995 0 0012 12.75a5.995 5.995 0 00-5.058 2.772m0 0a3 3 0 00-4.681 2.72 8.986 8.986 0 003.74.477m.94-3.197a5.971 5.971 0 00-.94 3.197M15 6.75a3 3 0 11-6 0 3 3 0 016 0Zm6 3a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0Zm-13.5 0a2.25 2.25 0 11-4.5 0 2.25 2.25 0 014.5 0Z"
    />
  </svg>
);

const DocumentIcon = () => (
  <svg
    xmlns="http://www.w3.org/2000/svg"
    fill="none"
    viewBox="0 0 24 24"
    strokeWidth={1.5}
    stroke="currentColor"
    className="size-5 text-[#063168] flex-shrink-0"
  >
    <path
      strokeLinecap="round"
      strokeLinejoin="round"
      d="M19.5 14.25v-2.625a3.375 3.375 0 00-3.375-3.375h-1.5A1.125 1.125 0 0113.5 7.125v-1.5a3.375 3.375 0 00-3.375-3.375H8.25m2.25 0H5.625c-.621 0-1.125.504-1.125 1.125v17.25c0 .621.504 1.125 1.125 1.125h12.75c.621 0 1.125-.504 1.125-1.125V11.25a9 9 0 00-9-9Z"
    />
  </svg>
);

const renderTeamSizeLabel = (eventData: EventPreviewData): string => {
  if (eventData.numTeammates <= 1) return "Individual Event";

  if (eventData.minTeammates === eventData.numTeammates) {
    return `Team Event (${eventData.numTeammates} members)`;
  }

  return `Team Event (${eventData.minTeammates}-${eventData.numTeammates} members)`;
};

const getPreviewErrorMessage = (draftKey: string | null): string => {
  if (!draftKey) {
    return "Missing preview key. Open preview from Create/Edit Event form.";
  }
  return "Preview draft was not found or has expired. Re-open preview from the form.";
};

function EventPreviewContent() {
  const searchParams = useSearchParams();
  const draftKey = searchParams.get("draft");

  const detailsRef = useRef<HTMLDivElement>(null);
  const rulesRef = useRef<HTMLDivElement>(null);
  const scheduleRef = useRef<HTMLDivElement>(null);
  const prizesRef = useRef<HTMLDivElement>(null);

  const [eventData, setEventData] = useState<EventPreviewData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    if (!draftKey) {
      setError(getPreviewErrorMessage(draftKey));
      setEventData(null);
      setLoading(false);
      return;
    }

    const draft = getEventPreviewDraft(draftKey);
    if (!draft) {
      setError(getPreviewErrorMessage(draftKey));
      setEventData(null);
      setLoading(false);
      return;
    }

    setEventData(draft);
    setError(null);
    setLoading(false);
  }, [draftKey]);

  const tags = useMemo(() => eventData?.tags || [], [eventData?.tags]);

  const scrollToSection = (
    ref: React.RefObject<HTMLDivElement | null>
  ): void => {
    ref.current?.scrollIntoView({ behavior: "smooth" });
  };

  if (loading) {
    return <EventPreviewLoadingFallback />;
  }

  if (!eventData || error) {
    return (
      <div className="min-h-[70vh] flex items-center justify-center p-4">
        <div className="max-w-lg w-full bg-white border-2 border-gray-200 rounded-xl p-6 text-center">
          <h1 className="text-xl font-bold text-[#063168] mb-3">Preview unavailable</h1>
          <p className="text-gray-600 mb-6">{error || "Could not load preview."}</p>
          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <Link
              href="/create/event"
              className="px-4 py-2.5 bg-[#154CB3] text-white rounded-lg font-medium hover:bg-[#0f3a7a] transition-colors"
            >
              Open Create Event
            </Link>
            <button
              type="button"
              onClick={() => window.close()}
              className="px-4 py-2.5 border border-gray-300 text-gray-700 rounded-lg font-medium hover:bg-gray-50 transition-colors"
            >
              Close Tab
            </button>
          </div>
        </div>
      </div>
    );
  }

  const showOutsiderBadge = Boolean(eventData.allow_outsiders);
  const hasRules = Boolean(eventData.rules && eventData.rules.length > 0);
  const hasSchedule = Boolean(eventData.schedule && eventData.schedule.length > 0);
  const hasPrizes = Boolean(eventData.prizes && eventData.prizes.length > 0);

  return (
    <div>
      <div className="px-4 sm:px-8 pt-6">
        <div className="max-w-6xl mx-auto rounded-xl border border-[#154CB3]/30 bg-[#F0F6FF] text-[#063168] px-4 py-3 text-sm font-medium flex flex-col sm:flex-row sm:items-center gap-3 sm:gap-4">
          <span className="inline-flex items-center gap-2">
            <span className="inline-flex w-2 h-2 rounded-full bg-[#154CB3]"></span>
            Preview mode only - this event is not published yet.
          </span>
          <div className="sm:ml-auto flex items-center gap-2">
            <button
              type="button"
              onClick={() => window.close()}
              className="px-3 py-1.5 rounded-md border border-[#154CB3] text-[#154CB3] hover:bg-blue-50 transition-colors"
            >
              Close Preview
            </button>
          </div>
        </div>
      </div>

      <div
        className="relative w-full h-[30vh] sm:h-[45vh] bg-cover bg-center bg-no-repeat mt-4"
        style={{
          backgroundImage: eventData.image ? `url('${eventData.image}')` : "none",
        }}
      >
        <div
          className="absolute inset-0 z-[1]"
          style={{ backgroundColor: "rgba(0, 0, 0, 0.4)" }}
        ></div>
        <div className="absolute inset-0 flex flex-col-reverse sm:flex-row justify-between p-4 sm:p-10 sm:px-12 items-end z-[2]">
          <div className="flex flex-col w-full sm:w-auto mt-4 sm:mt-0 sm:text-left">
            {(tags.length > 0 || showOutsiderBadge) && (
              <div className="flex flex-wrap gap-2 mb-2 items-center sm:justify-start">
                {showOutsiderBadge && (
                  <p className="px-2 sm:px-3 py-1 rounded-full text-xs sm:text-sm font-semibold bg-[#F59E0B] text-black">
                    Public
                  </p>
                )}
                {tags.map((tag, index) => (
                  <p
                    key={`${tag}-${index}`}
                    className={`px-2 sm:px-3 py-1 rounded-full text-xs sm:text-sm font-semibold ${
                      index % 2 === 0
                        ? "bg-[#FFCC00] text-black"
                        : "bg-[#063168] text-white"
                    }`}
                  >
                    {toTitleCase(tag)}
                  </p>
                ))}
              </div>
            )}
            <h1 className="text-[1.3rem] sm:text-[2.1rem] font-bold text-white m-0">
              {eventData.title}
            </h1>
            <p className="text-base sm:text-lg font-medium text-gray-200">
              {eventData.department}
            </p>
          </div>

          {eventData.daysLeft !== null && eventData.daysLeft >= 0 && (
            <div className="flex flex-col items-center bg-gradient-to-b from-[#FFCC00] to-[#FFE88D] rounded-xl border-2 border-[#FFCC0080] py-3 px-3 sm:px-4 sm:py-5 mb-4 sm:mb-0">
              <p className="text-3xl sm:text-5xl font-bold m-0 text-black">
                {eventData.daysLeft}
              </p>
              <p className="text-sm sm:text-base font-medium text-black">
                {eventData.daysLeft === 1 ? "day left" : "days left"}
              </p>
            </div>
          )}

          {eventData.daysLeft === null && (
            <div className="flex flex-col items-center bg-gradient-to-b from-[#22C55E] to-[#86EFAC] rounded-xl border-2 border-[#22C55E80] py-3 px-3 sm:px-4 sm:py-5 mb-4 sm:mb-0">
              <p className="text-lg sm:text-xl font-bold m-0 text-black">Open</p>
              <p className="text-sm sm:text-base font-medium text-black">
                Registration
              </p>
            </div>
          )}
        </div>
      </div>

      <div className="hidden sm:flex flex-col sm:flex-row flex-wrap px-4 sm:px-8 gap-4 sm:gap-8 text-gray-500 font-medium items-center bg-[#F5F5F5] h-auto sm:min-h-[10vh] m-4 sm:m-10 rounded-xl border-2 border-[#E0E0E0] py-4 sm:py-4 overflow-visible relative">
        <p
          className="text-[#063168] cursor-pointer transition-colors text-sm sm:text-base"
          onClick={() => scrollToSection(detailsRef)}
        >
          Details
        </p>
        {hasRules && (
          <p
            className="cursor-pointer hover:text-[#063168] transition-colors text-sm sm:text-base"
            onClick={() => scrollToSection(rulesRef)}
          >
            Rules and guidelines
          </p>
        )}
        {hasSchedule && (
          <p
            className="cursor-pointer hover:text-[#063168] transition-colors text-sm sm:text-base"
            onClick={() => scrollToSection(scheduleRef)}
          >
            Schedule
          </p>
        )}
        {hasPrizes && (
          <p
            className="cursor-pointer hover:text-[#063168] transition-colors text-sm sm:text-base"
            onClick={() => scrollToSection(prizesRef)}
          >
            Prizes
          </p>
        )}
        <div className="ml-auto flex flex-col items-end">
          <button
            type="button"
            disabled
            className="bg-[#154CB3] text-white py-2 sm:py-3 px-4 sm:px-6 rounded-full font-medium opacity-75 cursor-not-allowed text-sm sm:text-base"
          >
            Preview only
          </button>
        </div>
      </div>

      <div className="flex flex-col lg:flex-row gap-6 px-4 md:px-10 my-6 md:my-10">
        <div className="flex-1">
          <div className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden mb-6">
            <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
              <h2 className="text-xl font-semibold text-[#063168]">Event Details</h2>
            </div>
            <div ref={detailsRef} className="flex flex-col p-4 md:p-6 scroll-mt-24">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-y-4 gap-x-8">
                <div className="flex items-center gap-3">
                  <CalendarIcon />
                  <div>
                    <p className="text-sm text-gray-500">Date</p>
                    <p className="text-gray-800 font-medium">{eventData.date}</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <ClockIcon />
                  <div>
                    <p className="text-sm text-gray-500">Time</p>
                    <p className="text-gray-800 font-medium">{eventData.time}</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <CalendarIcon />
                  <div>
                    <p className="text-sm text-gray-500">End date</p>
                    <p className="text-gray-800 font-medium">{eventData.endDate}</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <LocationIcon />
                  <div>
                    <p className="text-sm text-gray-500">Location</p>
                    <p className="text-gray-800 font-medium">{eventData.location}</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <TicketIcon />
                  <div>
                    <p className="text-sm text-gray-500">Registration Fee</p>
                    <p className="text-gray-800 font-medium">{eventData.price}</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <UsersIcon />
                  <div>
                    <p className="text-sm text-gray-500">Team Size</p>
                    <p className="text-gray-800 font-medium">
                      {renderTeamSizeLabel(eventData)}
                    </p>
                  </div>
                </div>
                {eventData.pdf && (
                  <div className="flex items-center gap-3">
                    <DocumentIcon />
                    <div>
                      <p className="text-sm text-gray-500">Event Brochure</p>
                      <a
                        href={eventData.pdf}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-[#154CB3] hover:text-[#063168] hover:underline flex items-center font-medium"
                      >
                        Download PDF
                      </a>
                    </div>
                  </div>
                )}
              </div>

              <div className="mt-6 bg-slate-50 p-4 rounded-md border border-slate-200">
                <h3 className="text-lg font-medium text-[#063168] mb-2">About this event</h3>
                <p className="text-gray-700 whitespace-pre-line">{eventData.description}</p>
              </div>

              {eventData.organizers && eventData.organizers.length > 0 && (
                <div className="mt-6 pt-6 border-t border-gray-200 flex flex-col w-full">
                  <h3 className="text-lg font-medium text-[#063168] mb-4">Organizers</h3>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    {eventData.organizers.map((organizer, index) => (
                      <div
                        key={`${organizer.email}-${index}`}
                        className="p-4 bg-gray-50 rounded-md border border-gray-200"
                      >
                        <p className="font-semibold text-gray-800 text-md mb-1">{organizer.name}</p>
                        {organizer.email && organizer.email !== "N/A" && (
                          <p className="text-sm text-gray-600 mt-1 break-all">{organizer.email}</p>
                        )}
                        {organizer.phone && organizer.phone !== "N/A" && (
                          <p className="text-sm text-gray-600 mt-1">{organizer.phone}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>

          {hasRules && (
            <div className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden mb-6">
              <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
                <h2 className="text-xl font-semibold text-[#063168]">Rules & Guidelines</h2>
              </div>
              <div ref={rulesRef} className="p-4 md:p-6 scroll-mt-24">
                <ul className="space-y-3 list-disc list-inside marker:text-[#063168]">
                  {(eventData.rules || []).map((rule, index) => (
                    <li key={`${rule}-${index}`} className="text-gray-700">
                      {rule}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          )}

          {hasSchedule && (
            <div className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden mb-6">
              <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
                <h2 className="text-xl font-semibold text-[#063168]">Event Schedule</h2>
              </div>
              <div ref={scheduleRef} className="p-4 md:p-6 scroll-mt-24">
                {(eventData.schedule || []).map((item, index) => (
                  <div key={`${item.time}-${item.activity}-${index}`} className="flex gap-x-4">
                    <div
                      className={`relative ${
                        index === (eventData.schedule || []).length - 1
                          ? ""
                          : "after:absolute after:top-7 after:bottom-0 after:start-3.5 after:w-px after:-translate-x-[0.5px] after:bg-gray-300"
                      }`}
                    >
                      <div className="relative z-10 w-7 h-7 flex justify-center items-center">
                        <div className="w-3 h-3 rounded-full bg-[#063168] border-2 border-white"></div>
                      </div>
                    </div>
                    <div className="grow pt-0 pb-8">
                      <p className="text-md font-semibold text-[#063168] -mt-1">{item.activity}</p>
                      <p className="mt-1 text-sm text-gray-500">{item.time}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {hasPrizes && (
            <div className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden mb-6 lg:mb-0">
              <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
                <h2 className="text-xl font-semibold text-[#063168]">Prizes & Opportunities</h2>
              </div>
              <div ref={prizesRef} className="p-4 md:p-6 scroll-mt-24">
                <ul className="space-y-3">
                  {(eventData.prizes || []).map((prize, index) => (
                    <li key={`${prize}-${index}`} className="flex items-start gap-3">
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        fill="none"
                        viewBox="0 0 24 24"
                        strokeWidth={1.5}
                        stroke="currentColor"
                        className="w-5 h-5 text-[#FFCC00] flex-shrink-0 mt-0.5"
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          d="M16.5 18.75h-9m9 0a3 3 0 013 3h-15a3 3 0 013-3m9 0v-3.375c0-.621-.503-1.125-1.125-1.125h-.871M7.5 18.75v-3.375c0-.621.504-1.125 1.125-1.125h.872m5.007 0H9.497m5.007 0a7.454 7.454 0 01-.982-3.172M9.497 14.25a7.454 7.454 0 00.981-3.172M5.25 4.236c-.982.143-1.954.317-2.916.52A6.003 6.003 0 007.73 9.728M5.25 4.236V4.5c0 2.108.966 3.99 2.48 5.228M5.25 4.236V2.721C7.456 2.41 9.71 2.25 12 2.25c2.291 0 4.545.16 6.75.47v1.516M7.73 9.728a6.726 6.726 0 002.748 1.35m8.272-6.842V4.5c0 2.108-.966 3.99-2.48 5.228m2.48-5.492a46.32 46.32 0 012.916.52 6.003 6.003 0 01-5.395 4.972m0 0a6.726 6.726 0 01-2.749 1.35m0 0a6.772 6.772 0 01-3.044 0"
                        />
                      </svg>
                      <p className="text-gray-700">{prize}</p>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          )}

          <div className="mt-8 flex flex-col items-center pb-4">
            <button
              type="button"
              disabled
              className="bg-[#154CB3] text-white py-3 px-8 rounded-full font-semibold opacity-70 cursor-not-allowed text-base"
            >
              Preview only - publish from editor
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function EventPreviewLoadingFallback() {
  return (
    <div className="flex justify-center items-center min-h-[70vh]">
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
    </div>
  );
}

export default function EventPreviewPage() {
  return (
    <Suspense fallback={<EventPreviewLoadingFallback />}>
      <EventPreviewContent />
    </Suspense>
  );
}
