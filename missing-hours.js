'use strict';

const VALID_MISSING_HOURS_PERIODS = Object.freeze([
  'prev_week',
  'prev_month',
  'this_week',
  'this_month',
]);

function parseYmdStrict(value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(value || ''));
  if (!match) return null;

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return { year, month, day, date };
}

function formatYmdUtc(date) {
  return date.toISOString().slice(0, 10);
}

function addYmdDays(value, days) {
  const parsed = parseYmdStrict(value);
  if (!parsed || !Number.isInteger(days)) return null;
  return formatYmdUtc(new Date(parsed.date.getTime() + days * 86400000));
}

function resolvePeriodWindow(period, todayYmd) {
  const today = parseYmdStrict(todayYmd);
  if (!today || !VALID_MISSING_HOURS_PERIODS.includes(period)) return null;

  const todayUtc = today.date.getTime();
  const dow = today.date.getUTCDay();
  const mondayOffset = dow === 0 ? -6 : 1 - dow;

  if (period === 'prev_week') {
    const thisMonday = todayUtc + mondayOffset * 86400000;
    return {
      from: formatYmdUtc(new Date(thisMonday - 7 * 86400000)),
      to: formatYmdUtc(new Date(thisMonday - 86400000)),
    };
  }

  if (period === 'prev_month') {
    const firstOfThisMonth = Date.UTC(today.year, today.month - 1, 1);
    const lastOfPrevMonth = new Date(firstOfThisMonth - 86400000);
    return {
      from: formatYmdUtc(
        new Date(
          Date.UTC(
            lastOfPrevMonth.getUTCFullYear(),
            lastOfPrevMonth.getUTCMonth(),
            1,
          ),
        ),
      ),
      to: formatYmdUtc(lastOfPrevMonth),
    };
  }

  if (period === 'this_week') {
    return {
      from: formatYmdUtc(new Date(todayUtc + mondayOffset * 86400000)),
      to: todayYmd,
    };
  }

  return {
    from: `${String(today.year).padStart(4, '0')}-${String(today.month).padStart(2, '0')}-01`,
    to: todayYmd,
  };
}

function resolveMissingHoursRange({ from, to, period, todayYmd }) {
  const hasExplicitRange = Boolean(from && to);
  let requestedFrom = from;
  let requestedTo = to;

  if (hasExplicitRange) {
    if (!parseYmdStrict(from) || !parseYmdStrict(to) || to < from) return null;
  } else {
    const window = resolvePeriodWindow(period, todayYmd);
    if (!window) return null;
    requestedFrom = window.from;
    requestedTo = window.to;
  }

  const isCurrentPeriod =
    !hasExplicitRange && (period === 'this_week' || period === 'this_month');
  const completedTo = isCurrentPeriod
    ? addYmdDays(todayYmd, -1)
    : requestedTo;
  const evaluatedTo =
    completedTo && completedTo >= requestedFrom ? completedTo : null;

  return {
    requestedFrom,
    requestedTo,
    evaluatedFrom: requestedFrom,
    evaluatedTo,
    excludedTo: isCurrentPeriod ? todayYmd : null,
    hasCompletedDays: evaluatedTo !== null,
  };
}

function buildMissingHoursRows(hours, threshold) {
  const {
    weekdayHours,
    sharedOffHours,
    requiredHours,
    ptoByName,
    loggedByName,
    users,
  } = hours;

  const rows = users.map((user) => {
    const nameKey = user.name ? user.name.trim().toLowerCase() : '';
    const ptoHours = ptoByName.get(nameKey) ?? 0;
    const loggedHours = loggedByName.get(nameKey) ?? 0;
    const missing = requiredHours - ptoHours - loggedHours;
    return {
      name: user.name || user.email,
      email: user.email,
      weekdayHours,
      sharedOffHours,
      requiredHours,
      ptoHours,
      loggedHours,
      missing,
      overThreshold: missing > threshold,
    };
  });

  rows.sort((a, b) => b.missing - a.missing);
  return rows;
}

module.exports = {
  VALID_MISSING_HOURS_PERIODS,
  buildMissingHoursRows,
  resolveMissingHoursRange,
};
