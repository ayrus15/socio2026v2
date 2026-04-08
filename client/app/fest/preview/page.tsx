"use client";

import Link from "next/link";
import { Suspense, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";
import {
  FestPreviewData,
  getFestPreviewDraft,
} from "@/app/lib/festPreviewDraft";

const toTitleCase = (value: string): string =>
  value
    .split(" ")
    .map((chunk) =>
      chunk.length > 0
        ? `${chunk[0].toUpperCase()}${chunk.slice(1).toLowerCase()}`
        : chunk
    )
    .join(" ");

const getPreviewErrorMessage = (draftKey: string | null): string => {
  if (!draftKey) {
    return "Missing preview key. Open preview from Create/Edit Fest form.";
  }
  return "Preview draft was not found or has expired. Re-open preview from the form.";
};

const renderTeamFormat = (festData: FestPreviewData): string => {
  if (!festData.isTeamEvent || festData.maxParticipants <= 1) {
    return "Individual entries";
  }

  if (festData.minParticipants === festData.maxParticipants) {
    return `${festData.maxParticipants} members per team`;
  }

  return `${festData.minParticipants}-${festData.maxParticipants} members per team`;
};

function FestPreviewContent() {
  const searchParams = useSearchParams();
  const draftKey = searchParams.get("draft");

  const [festData, setFestData] = useState<FestPreviewData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);

    if (!draftKey) {
      setError(getPreviewErrorMessage(draftKey));
      setFestData(null);
      setLoading(false);
      return;
    }

    const draft = getFestPreviewDraft(draftKey);
    if (!draft) {
      setError(getPreviewErrorMessage(draftKey));
      setFestData(null);
      setLoading(false);
      return;
    }

    setFestData(draft);
    setError(null);
    setLoading(false);
  }, [draftKey]);

  const badges = useMemo(() => {
    if (!festData) return [];

    return [
      festData.category ? toTitleCase(festData.category) : null,
      festData.campusHostedAt || null,
      festData.allowOutsiders ? "Public" : null,
    ].filter((item): item is string => Boolean(item && item.trim()));
  }, [festData]);

  if (loading) {
    return <FestPreviewLoadingFallback />;
  }

  if (!festData || error) {
    return (
      <div className="min-h-[70vh] flex items-center justify-center p-4">
        <div className="max-w-lg w-full bg-white border-2 border-gray-200 rounded-xl p-6 text-center">
          <h1 className="text-xl font-bold text-[#063168] mb-3">Preview unavailable</h1>
          <p className="text-gray-600 mb-6">{error || "Could not load preview."}</p>
          <div className="flex flex-col sm:flex-row gap-3 justify-center">
            <Link
              href="/create/fest"
              className="px-4 py-2.5 bg-[#154CB3] text-white rounded-lg font-medium hover:bg-[#0f3a7a] transition-colors"
            >
              Open Create Fest
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

  const hasTimeline = Boolean(festData.timeline && festData.timeline.length > 0);
  const hasSponsors = Boolean(festData.sponsors && festData.sponsors.length > 0);
  const hasSocialLinks = Boolean(
    festData.socialLinks && festData.socialLinks.length > 0
  );
  const hasFaqs = Boolean(festData.faqs && festData.faqs.length > 0);
  const hasEventHeads = Boolean(festData.eventHeads && festData.eventHeads.length > 0);

  return (
    <div className="min-h-screen bg-white pb-8">
      <div className="px-4 sm:px-8 pt-6">
        <div className="max-w-6xl mx-auto rounded-xl border border-[#154CB3]/30 bg-[#F0F6FF] text-[#063168] px-4 py-3 text-sm font-medium flex flex-col sm:flex-row sm:items-center gap-3 sm:gap-4">
          <span className="inline-flex items-center gap-2">
            <span className="inline-flex w-2 h-2 rounded-full bg-[#154CB3]"></span>
            Preview mode only - this fest is not published yet.
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
        style={{ backgroundImage: festData.image ? `url('${festData.image}')` : "none" }}
      >
        <div
          className="absolute inset-0 z-[1]"
          style={{ backgroundColor: "rgba(0, 0, 0, 0.45)" }}
        ></div>

        <div className="absolute inset-0 flex flex-col justify-end p-4 sm:p-10 sm:px-12 z-[2]">
          {badges.length > 0 && (
            <div className="flex flex-wrap gap-2 mb-3">
              {badges.map((badge, index) => (
                <p
                  key={`${badge}-${index}`}
                  className={`px-2 sm:px-3 py-1 rounded-full text-xs sm:text-sm font-semibold ${
                    index % 2 === 0
                      ? "bg-[#FFCC00] text-black"
                      : "bg-[#063168] text-white"
                  }`}
                >
                  {badge}
                </p>
              ))}
            </div>
          )}

          <h1 className="text-[1.3rem] sm:text-[2.1rem] font-bold text-white m-0">
            {festData.title}
          </h1>
          <p className="text-base sm:text-lg font-medium text-gray-200">
            {festData.organizingDept}
          </p>
        </div>
      </div>

      <main className="max-w-6xl mx-auto px-4 sm:px-8 mt-8 space-y-6">
        <section className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden">
          <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
            <h2 className="text-xl font-semibold text-[#063168]">Fest Details</h2>
          </div>
          <div className="p-4 md:p-6 space-y-6">
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
              <div className="bg-slate-50 rounded-md border border-slate-200 p-3">
                <p className="text-xs text-gray-500 uppercase tracking-wide">Opening Date</p>
                <p className="text-sm font-medium text-gray-800 mt-1">{festData.openingDate}</p>
              </div>
              <div className="bg-slate-50 rounded-md border border-slate-200 p-3">
                <p className="text-xs text-gray-500 uppercase tracking-wide">Closing Date</p>
                <p className="text-sm font-medium text-gray-800 mt-1">{festData.closingDate}</p>
              </div>
              <div className="bg-slate-50 rounded-md border border-slate-200 p-3">
                <p className="text-xs text-gray-500 uppercase tracking-wide">Venue</p>
                <p className="text-sm font-medium text-gray-800 mt-1">{festData.venue || "Venue TBD"}</p>
              </div>
              <div className="bg-slate-50 rounded-md border border-slate-200 p-3">
                <p className="text-xs text-gray-500 uppercase tracking-wide">Team Format</p>
                <p className="text-sm font-medium text-gray-800 mt-1">{renderTeamFormat(festData)}</p>
              </div>
            </div>

            <div className="bg-slate-50 p-4 rounded-md border border-slate-200">
              <h3 className="text-lg font-medium text-[#063168] mb-2">About this fest</h3>
              <p className="text-gray-700 whitespace-pre-line">{festData.description}</p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="bg-white rounded-md border border-gray-200 p-4">
                <h4 className="text-sm font-semibold text-[#063168] mb-2">Contact Email</h4>
                <p className="text-sm text-gray-700 break-all">{festData.contactEmail || "Not provided"}</p>
              </div>
              <div className="bg-white rounded-md border border-gray-200 p-4">
                <h4 className="text-sm font-semibold text-[#063168] mb-2">Contact Phone</h4>
                <p className="text-sm text-gray-700">{festData.contactPhone || "Not provided"}</p>
              </div>
            </div>

            {festData.allowedCampuses && festData.allowedCampuses.length > 0 && (
              <div className="bg-white rounded-md border border-gray-200 p-4">
                <h4 className="text-sm font-semibold text-[#063168] mb-2">Allowed Campuses</h4>
                <div className="flex flex-wrap gap-2">
                  {festData.allowedCampuses.map((campus, index) => (
                    <span
                      key={`${campus}-${index}`}
                      className="px-2.5 py-1 rounded-full text-xs font-medium bg-blue-50 text-[#154CB3] border border-blue-100"
                    >
                      {campus}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {hasEventHeads && (
              <div className="bg-white rounded-md border border-gray-200 p-4">
                <h4 className="text-sm font-semibold text-[#063168] mb-3">Event Heads</h4>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {(festData.eventHeads || []).map((head, index) => (
                    <div key={`${head.email}-${index}`} className="p-3 rounded-md border border-gray-200 bg-slate-50">
                      <p className="text-sm font-medium text-gray-800 break-all">{head.email}</p>
                      {head.expiresAt && (
                        <p className="text-xs text-gray-500 mt-1">Access until: {head.expiresAt}</p>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </section>

        {hasTimeline && (
          <section className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden">
            <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
              <h2 className="text-xl font-semibold text-[#063168]">Timeline</h2>
            </div>
            <div className="p-4 md:p-6">
              {(festData.timeline || []).map((item, index) => (
                <div key={`${item.time}-${item.title}-${index}`} className="flex gap-x-4">
                  <div
                    className={`relative ${
                      index === (festData.timeline || []).length - 1
                        ? ""
                        : "after:absolute after:top-7 after:bottom-0 after:start-3.5 after:w-px after:-translate-x-[0.5px] after:bg-gray-300"
                    }`}
                  >
                    <div className="relative z-10 w-7 h-7 flex justify-center items-center">
                      <div className="w-3 h-3 rounded-full bg-[#063168] border-2 border-white"></div>
                    </div>
                  </div>
                  <div className="grow pt-0 pb-8">
                    <p className="text-md font-semibold text-[#063168] -mt-1">
                      {item.title || "Timeline Item"}
                    </p>
                    {item.time && <p className="mt-1 text-sm text-gray-500">{item.time}</p>}
                    {item.description && (
                      <p className="mt-2 text-sm text-gray-700">{item.description}</p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </section>
        )}

        {hasSponsors && (
          <section className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden">
            <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
              <h2 className="text-xl font-semibold text-[#063168]">Sponsors</h2>
            </div>
            <div className="p-4 md:p-6 grid grid-cols-1 md:grid-cols-2 gap-4">
              {(festData.sponsors || []).map((sponsor, index) => (
                <div
                  key={`${sponsor.name}-${index}`}
                  className="border border-gray-200 rounded-lg p-4 bg-slate-50"
                >
                  <div className="flex items-start gap-3">
                    {sponsor.logo_url ? (
                      <img
                        src={sponsor.logo_url}
                        alt={sponsor.name || "Sponsor"}
                        className="w-14 h-14 object-cover rounded-md border border-gray-200 bg-white"
                      />
                    ) : (
                      <div className="w-14 h-14 rounded-md border border-gray-200 bg-white flex items-center justify-center text-xs text-gray-400">
                        Logo
                      </div>
                    )}
                    <div className="min-w-0">
                      <p className="text-sm font-semibold text-gray-800 break-words">
                        {sponsor.name || "Unnamed Sponsor"}
                      </p>
                      {sponsor.website && (
                        <a
                          href={sponsor.website}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-xs text-[#154CB3] hover:underline break-all"
                        >
                          {sponsor.website}
                        </a>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </section>
        )}

        {hasSocialLinks && (
          <section className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden">
            <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
              <h2 className="text-xl font-semibold text-[#063168]">Social Links</h2>
            </div>
            <div className="p-4 md:p-6 space-y-3">
              {(festData.socialLinks || []).map((item, index) => (
                <div
                  key={`${item.platform}-${item.url}-${index}`}
                  className="flex flex-col sm:flex-row sm:items-center gap-1 sm:gap-3 p-3 rounded-md border border-gray-200 bg-slate-50"
                >
                  <span className="text-sm font-semibold text-gray-800 min-w-[120px]">
                    {toTitleCase(item.platform || "Link")}
                  </span>
                  {item.url ? (
                    <a
                      href={item.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-sm text-[#154CB3] hover:underline break-all"
                    >
                      {item.url}
                    </a>
                  ) : (
                    <span className="text-sm text-gray-500">No URL provided</span>
                  )}
                </div>
              ))}
            </div>
          </section>
        )}

        {hasFaqs && (
          <section className="bg-white rounded-lg border-2 border-gray-200 overflow-hidden">
            <div className="border-b border-gray-200 bg-[#F5F5F5] p-4 md:p-6">
              <h2 className="text-xl font-semibold text-[#063168]">Frequently Asked Questions</h2>
            </div>
            <div className="p-4 md:p-6 space-y-4">
              {(festData.faqs || []).map((faq, index) => (
                <div key={`${faq.question}-${index}`} className="border border-gray-200 rounded-lg p-4 bg-slate-50">
                  <p className="text-sm font-semibold text-gray-800">
                    {faq.question || `Question ${index + 1}`}
                  </p>
                  {faq.answer && <p className="text-sm text-gray-700 mt-2">{faq.answer}</p>}
                </div>
              ))}
            </div>
          </section>
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
      </main>
    </div>
  );
}

function FestPreviewLoadingFallback() {
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

export default function FestPreviewPage() {
  return (
    <Suspense fallback={<FestPreviewLoadingFallback />}>
      <FestPreviewContent />
    </Suspense>
  );
}
