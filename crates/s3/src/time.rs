//! Times as S3 reads and writes them: `YYYYMMDDTHHMMSSZ` in signatures (05 §1.6),
//! HTTP-dates in headers (RFC 9110 §5.6.7), and ISO 8601 with milliseconds in XML bodies, as
//! in AWS's `<LastModified>2009-10-12T17:50:30.000Z</LastModified>` (05 §6.2).
//!
//! Calendar arithmetic is Hinnant's "chrono-Compatible Low-Level Date Algorithms" over the
//! proleptic Gregorian calendar, checked throughout; a value outside the four-digit years
//! these formats hold is `None`.

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Day names from Sunday; 1970-01-01, day 0, was a Thursday.
const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

const SECONDS_PER_DAY: i64 = 86_400;

/// `YYYYMMDDTHHMMSSZ` as Unix seconds (05 §1.6: UTC, no fractional seconds).
pub fn parse_amz_date(text: &str) -> Option<i64> {
    let b = text.as_bytes();
    if b.len() != 16 || b.get(8) != Some(&b'T') || b.get(15) != Some(&b'Z') {
        return None;
    }
    let num = |from: usize, to: usize| number(text.get(from..to)?);
    unix(
        num(0, 4)?,
        num(4, 6)?,
        num(6, 8)?,
        num(9, 11)?,
        num(11, 13)?,
        num(13, 15)?,
    )
}

/// An HTTP-date as Unix seconds, in any of the three forms recipients must accept (RFC 9110
/// §5.6.7): `Sun, 06 Nov 1994 08:49:37 GMT`, `Sunday, 06-Nov-94 08:49:37 GMT` and
/// `Sun Nov  6 08:49:37 1994`.
pub fn parse_http_date(text: &str) -> Option<i64> {
    let words: Vec<&str> = text.split_whitespace().collect();
    let (day, month, year, time) = match words.as_slice() {
        [_, day, month, year, time, "GMT"] => (*day, *month, number(year)?, *time),
        [_, date, time, "GMT"] => {
            let mut parts = date.split('-');
            let day = parts.next()?;
            let month = parts.next()?;
            let yy = number(parts.next()?)?;
            // RFC 9110 §5.6.7: a two-digit year more than 50 years in the future is the past
            // century's; taken here as 1970–2069 around the epoch these dates describe.
            let year = if yy < 70 {
                yy.checked_add(2000)?
            } else {
                yy.checked_add(1900)?
            };
            (day, month, year, *time)
        }
        [_, month, day, time, year] => (*day, *month, number(year)?, *time),
        _ => return None,
    };
    let month = MONTHS.iter().position(|m| *m == month)?;
    let month = i64::try_from(month).ok()?.checked_add(1)?;
    let mut hms = time.split(':').map(number);
    let (h, m, s) = (hms.next()??, hms.next()??, hms.next()??);
    if hms.next().is_some() {
        return None;
    }
    unix(year, month, number(day)?, h, m, s)
}

/// Unix seconds as an HTTP-date's preferred form, IMF-fixdate (RFC 9110 §5.6.7):
/// `Sun, 06 Nov 1994 08:49:37 GMT`.
pub fn http_date(seconds: i64) -> Option<String> {
    let days = seconds.checked_div_euclid(SECONDS_PER_DAY)?;
    let (year, month, day) = civil(days)?;
    let (h, m, s) = clock(seconds.checked_rem_euclid(SECONDS_PER_DAY)?)?;
    let weekday = DAYS.get(usize::try_from(days.checked_add(4)?.checked_rem_euclid(7)?).ok()?)?;
    let month = MONTHS.get(usize::try_from(month.checked_sub(1)?).ok()?)?;
    Some(format!(
        "{weekday}, {day:02} {month} {year:04} {h:02}:{m:02}:{s:02} GMT"
    ))
}

/// Unix milliseconds as S3 writes a time in XML: `2009-10-12T17:50:30.000Z`.
pub fn iso8601(millis: i64) -> Option<String> {
    let seconds = millis.checked_div_euclid(1000)?;
    let fraction = millis.checked_rem_euclid(1000)?;
    let (year, month, day) = civil(seconds.checked_div_euclid(SECONDS_PER_DAY)?)?;
    let (h, m, s) = clock(seconds.checked_rem_euclid(SECONDS_PER_DAY)?)?;
    Some(format!(
        "{year:04}-{month:02}-{day:02}T{h:02}:{m:02}:{s:02}.{fraction:03}Z"
    ))
}

/// Digits alone as a number: no sign, no space.
fn number(text: &str) -> Option<i64> {
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

/// A UTC calendar time as Unix seconds, `None` unless every field is in range. A leap second,
/// `:60`, is the next minute's first.
fn unix(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64) -> Option<i64> {
    if !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }
    let seconds = hour
        .checked_mul(3600)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?;
    days_from_civil(year, month, day)?
        .checked_mul(SECONDS_PER_DAY)?
        .checked_add(seconds)
}

/// Hours, minutes and seconds of a second of the day.
fn clock(second_of_day: i64) -> Option<(i64, i64, i64)> {
    Some((
        second_of_day.checked_div(3600)?,
        second_of_day.checked_rem(3600)?.checked_div(60)?,
        second_of_day.checked_rem(60)?,
    ))
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days from 1970-01-01 to a proleptic Gregorian date (Hinnant, `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    let y = if month <= 2 {
        year.checked_sub(1)?
    } else {
        year
    };
    let era = y.checked_div_euclid(400)?;
    let yoe = y.checked_sub(era.checked_mul(400)?)?;
    let mp = month.checked_add(9)?.checked_rem(12)?;
    let doy = mp
        .checked_mul(153)?
        .checked_add(2)?
        .checked_div(5)?
        .checked_add(day)?
        .checked_sub(1)?;
    let doe = yoe
        .checked_mul(365)?
        .checked_add(yoe.checked_div(4)?)?
        .checked_sub(yoe.checked_div(100)?)?
        .checked_add(doy)?;
    era.checked_mul(146_097)?
        .checked_add(doe)?
        .checked_sub(719_468)
}

/// The proleptic Gregorian date of a day counted from 1970-01-01 (Hinnant,
/// `civil_from_days`), within the years 0000–9999 the formats here write.
fn civil(days: i64) -> Option<(i64, i64, i64)> {
    let z = days.checked_add(719_468)?;
    let era = z.checked_div_euclid(146_097)?;
    let doe = z.checked_rem_euclid(146_097)?;
    let yoe = doe
        .checked_sub(doe.checked_div(1460)?)?
        .checked_add(doe.checked_div(36_524)?)?
        .checked_sub(doe.checked_div(146_096)?)?
        .checked_div(365)?;
    let doy = doe.checked_sub(
        yoe.checked_mul(365)?
            .checked_add(yoe.checked_div(4)?)?
            .checked_sub(yoe.checked_div(100)?)?,
    )?;
    let mp = doy.checked_mul(5)?.checked_add(2)?.checked_div(153)?;
    let day = doy
        .checked_sub(mp.checked_mul(153)?.checked_add(2)?.checked_div(5)?)?
        .checked_add(1)?;
    let month = if mp < 10 {
        mp.checked_add(3)?
    } else {
        mp.checked_sub(9)?
    };
    let year = yoe
        .checked_add(era.checked_mul(400)?)?
        .checked_add(i64::from(month <= 2))?;
    if !(0..=9999).contains(&year) {
        return None;
    }
    Some((year, month, day))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amz_dates_parse_as_utc() {
        assert_eq!(parse_amz_date("19700101T000000Z"), Some(0));
        assert_eq!(parse_amz_date("20130524T000000Z"), Some(1_369_353_600));
        assert_eq!(parse_amz_date("20240229T235959Z"), Some(1_709_251_199));
        assert_eq!(parse_amz_date("20230229T000000Z"), None);
        assert_eq!(parse_amz_date("2013-05-24T00:00:00Z"), None);
        assert_eq!(parse_amz_date("20130524T-10000Z"), None);
        assert_eq!(parse_amz_date("2013+524T000000Z"), None);
    }

    #[test]
    fn http_dates_parse_in_all_three_forms() {
        let want = Some(784_111_777);
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"), want);
        assert_eq!(parse_http_date("Sunday, 06-Nov-94 08:49:37 GMT"), want);
        assert_eq!(parse_http_date("Sun Nov  6 08:49:37 1994"), want);
        assert_eq!(parse_http_date("06 Nov 1994"), None);
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:+7 GMT"), None);
    }

    /// RFC 9110 §5.6.7's example, and AWS's listing example (05 §6.2).
    #[test]
    fn times_format_as_s3_writes_them() {
        assert_eq!(
            http_date(784_111_777).as_deref(),
            Some("Sun, 06 Nov 1994 08:49:37 GMT")
        );
        let listed = parse_amz_date("20091012T175030Z").unwrap();
        assert_eq!(
            iso8601(listed * 1000).as_deref(),
            Some("2009-10-12T17:50:30.000Z")
        );
        assert_eq!(iso8601(-1).as_deref(), Some("1969-12-31T23:59:59.999Z"));
        assert_eq!(iso8601(i64::MAX), None);
        assert_eq!(http_date(i64::MIN), None);
    }

    /// Every day from 0000-01-01 to 9999-12-31 converts both ways.
    #[test]
    fn the_calendar_round_trips() {
        let first = days_from_civil(0, 1, 1).unwrap();
        let last = days_from_civil(9999, 12, 31).unwrap();
        let (mut year, mut month, mut day) = (0, 1, 1);
        for days in first..=last {
            assert_eq!(civil(days), Some((year, month, day)), "{days}");
            assert_eq!(days_from_civil(year, month, day), Some(days));
            day += 1;
            if day > days_in_month(year, month) {
                day = 1;
                month += 1;
                if month > 12 {
                    month = 1;
                    year += 1;
                }
            }
        }
        assert_eq!(civil(last + 1), None);
        assert_eq!(civil(first - 1), None);
    }
}
