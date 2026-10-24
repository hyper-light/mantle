//! A certificate's times (RFC 5280 §4.1.2.5): UTCTime `YYMMDDHHMMSSZ` through 2049 and
//! GeneralizedTime `YYYYMMDDHHMMSSZ` from 2050, both in UTC with seconds and no fraction, from
//! seconds since the Unix epoch and back. The civil date is Hinnant's `civil_from_days`, and its
//! inverse `days_from_civil` ("chrono-Compatible Low-Level Date Algorithms", proleptic Gregorian
//! calendar), over non-negative days only.

use crate::CertError;
use crate::der;

const SECONDS_A_DAY: u64 = 86_400;

/// The years RFC 5280 lets a certificate state: UTCTime's two digits reach back to 1950
/// (§4.1.2.5.1), and GeneralizedTime's four forward to 9999.
const FIRST_YEAR: u64 = 1950;
const GENERALIZED_FROM: u64 = 2050;
const LAST_YEAR: u64 = 9999;

/// A civil date and time of day, UTC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Civil {
    pub(crate) year: u64,
    pub(crate) month: u64,
    pub(crate) day: u64,
    pub(crate) hour: u64,
    pub(crate) minute: u64,
    pub(crate) second: u64,
}

fn checked(v: Option<u64>) -> Result<u64, CertError> {
    v.ok_or(CertError::Time)
}

/// The civil date and time `unix` seconds after 1970-01-01T00:00:00Z.
pub(crate) fn civil(unix: u64) -> Result<Civil, CertError> {
    let days = unix / SECONDS_A_DAY;
    let rest = unix % SECONDS_A_DAY;
    // Hinnant's civil_from_days, shifted to eras of 400 years starting 0000-03-01, every value
    // non-negative for days from 1970.
    let z = checked(days.checked_add(719_468))?;
    let era = z / 146_097;
    let doe = checked(z.checked_sub(checked(era.checked_mul(146_097))?))?;
    let yoe = checked(
        doe.checked_sub(doe / 1_460)
            .and_then(|v| v.checked_add(doe / 36_524))
            .and_then(|v| v.checked_sub(doe / 146_096)),
    )? / 365;
    let year = checked(yoe.checked_add(checked(era.checked_mul(400))?))?;
    let doy = checked(
        doe.checked_sub(checked(
            yoe.checked_mul(365)
                .and_then(|v| v.checked_add(yoe / 4))
                .and_then(|v| v.checked_sub(yoe / 100)),
        )?),
    )?;
    let mp = checked(doy.checked_mul(5).and_then(|v| v.checked_add(2)))? / 153;
    let day = checked(
        doy.checked_sub(checked(mp.checked_mul(153).and_then(|v| v.checked_add(2)))? / 5)
            .and_then(|v| v.checked_add(1)),
    )?;
    let month = if mp < 10 {
        checked(mp.checked_add(3))?
    } else {
        checked(mp.checked_sub(9))?
    };
    let year = if month <= 2 {
        checked(year.checked_add(1))?
    } else {
        year
    };
    Ok(Civil {
        year,
        month,
        day,
        hour: rest / 3_600,
        minute: rest % 3_600 / 60,
        second: rest % 60,
    })
}

/// `value`'s last `width` decimal digits, as ASCII.
fn digits(out: &mut Vec<u8>, value: u64, width: u32) -> Result<(), CertError> {
    let mut divisor = checked(10u64.checked_pow(width))?;
    while divisor > 1 {
        divisor /= 10;
        let digit = checked(value.checked_div(divisor))? % 10;
        out.push(b'0' | u8::try_from(digit).map_err(|_| CertError::Time)?);
    }
    Ok(())
}

/// Appends the time `unix` seconds after the epoch as RFC 5280 states it: UTCTime through 2049,
/// GeneralizedTime from 2050; a year before 1950 or after 9999 cannot be stated.
pub(crate) fn encode(out: &mut Vec<u8>, unix: u64) -> Result<(), CertError> {
    let c = civil(unix)?;
    if c.year < FIRST_YEAR || c.year > LAST_YEAR {
        return Err(CertError::Time);
    }
    let mut text = Vec::new();
    let tag = if c.year < GENERALIZED_FROM {
        digits(&mut text, c.year, 2)?;
        der::UTC_TIME
    } else {
        digits(&mut text, c.year, 4)?;
        der::GENERALIZED_TIME
    };
    for field in [c.month, c.day, c.hour, c.minute, c.second] {
        digits(&mut text, field, 2)?;
    }
    text.push(b'Z');
    der::tlv(out, tag, &text)
}

/// The seconds after the Unix epoch a certificate's time states (RFC 5280 §4.1.2.5): a UTCTime
/// `YYMMDDHHMMSSZ`, its year 1950 to 2049 (§4.1.2.5.1), or a GeneralizedTime `YYYYMMDDHHMMSSZ`
/// (§4.1.2.5.2), each in UTC with seconds and no fraction, the forms this profile writes; any
/// other, or a date the calendar has not, is refused. A time before the epoch states 0: the start's
/// check compares it with a clock read after the epoch, which is past it either way.
pub(crate) fn decode(read: &der::Read<'_>) -> Result<u64, CertError> {
    let (year, rest) = match read.tag {
        der::UTC_TIME => {
            let (yy, rest) = read
                .content
                .split_at_checked(2)
                .ok_or(CertError::Malformed)?;
            let yy = number(yy)?;
            let century = if yy >= FIRST_YEAR % 100 { 1900 } else { 2000 };
            (checked(yy.checked_add(century))?, rest)
        }
        der::GENERALIZED_TIME => {
            let (yyyy, rest) = read
                .content
                .split_at_checked(4)
                .ok_or(CertError::Malformed)?;
            (number(yyyy)?, rest)
        }
        _ => return Err(CertError::Malformed),
    };
    let &[m1, m2, d1, d2, h1, h2, n1, n2, s1, s2, b'Z'] = rest else {
        return Err(CertError::Malformed);
    };
    let field = |a: u8, b: u8| number(&[a, b]);
    let (month, day) = (field(m1, m2)?, field(d1, d2)?);
    let (hour, minute, second) = (field(h1, h2)?, field(n1, n2)?, field(s1, s2)?);
    if !(1..=12).contains(&month)
        || day == 0
        || day > days_in_month(year, month)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(CertError::Malformed);
    }
    let Some(days) = days_from_civil(year, month, day)? else {
        return Ok(0);
    };
    let within = checked(
        hour.checked_mul(3_600)
            .zip(minute.checked_mul(60))
            .and_then(|(h, m)| h.checked_add(m))
            .and_then(|hm| hm.checked_add(second)),
    )?;
    checked(
        days.checked_mul(SECONDS_A_DAY)
            .and_then(|s| s.checked_add(within)),
    )
}

/// The decimal number `text`'s ASCII digits state, refused where any is not a digit.
fn number(text: &[u8]) -> Result<u64, CertError> {
    let mut value = 0u64;
    for &digit in text {
        if !digit.is_ascii_digit() {
            return Err(CertError::Malformed);
        }
        value = checked(
            value
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(digit & 0x0f))),
        )?;
    }
    Ok(value)
}

/// The days of `month` in `year` of the proleptic Gregorian calendar.
fn days_in_month(year: u64, month: u64) -> u64 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => 31,
    }
}

/// The days from 1970-01-01 to the date, by Hinnant's `days_from_civil`; `None` for a date before
/// 1970.
fn days_from_civil(year: u64, month: u64, day: u64) -> Result<Option<u64>, CertError> {
    // March first: January and February count as the previous year's thirteenth and fourteenth.
    let year = if month <= 2 {
        checked(year.checked_sub(1))?
    } else {
        year
    };
    let era = year / 400;
    let yoe = checked(year.checked_sub(checked(era.checked_mul(400))?))?;
    let shifted = if month > 2 {
        checked(month.checked_sub(3))?
    } else {
        checked(month.checked_add(9))?
    };
    let doy = checked(
        shifted
            .checked_mul(153)
            .and_then(|v| v.checked_add(2))
            .map(|v| v / 5)
            .and_then(|v| v.checked_add(day))
            .and_then(|v| v.checked_sub(1)),
    )?;
    let doe = checked(
        yoe.checked_mul(365)
            .and_then(|v| v.checked_add(yoe / 4))
            .and_then(|v| v.checked_sub(yoe / 100))
            .and_then(|v| v.checked_add(doy)),
    )?;
    let days = checked(era.checked_mul(146_097).and_then(|v| v.checked_add(doe)))?;
    Ok(days.checked_sub(719_468))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(unix: u64) -> (u8, String) {
        let mut out = Vec::new();
        encode(&mut out, unix).unwrap();
        (out[0], String::from_utf8(out[2..].to_vec()).unwrap())
    }

    /// Dates checked against `date -u -r SECONDS` and RFC 5280's own boundaries.
    #[test]
    fn times_read_as_the_calendar_states_them() {
        assert_eq!(text(0), (der::UTC_TIME, "700101000000Z".into()));
        // 2000-02-29, a leap day of a century divisible by 400.
        assert_eq!(text(951_782_400), (der::UTC_TIME, "000229000000Z".into()));
        // 2026-10-19T12:34:56Z.
        assert_eq!(text(1_792_413_296), (der::UTC_TIME, "261019123456Z".into()));
        // 2049-12-31T23:59:59Z, UTCTime's last second, and 2050-01-01, GeneralizedTime's first.
        assert_eq!(text(2_524_607_999), (der::UTC_TIME, "491231235959Z".into()));
        assert_eq!(
            text(2_524_608_000),
            (der::GENERALIZED_TIME, "20500101000000Z".into())
        );
        // 2100-03-01, after a century year that is not a leap year.
        assert_eq!(
            text(4_107_542_400),
            (der::GENERALIZED_TIME, "21000301000000Z".into())
        );
    }

    #[test]
    fn a_year_past_9999_cannot_be_stated() {
        // 10000-01-01T00:00:00Z.
        let mut out = Vec::new();
        assert!(encode(&mut out, 253_402_300_800).is_err());
    }

    /// Every time [`encode`] states reads back as the same second, across both forms, leap days and
    /// the century years, and the forms a calendar has no date for, or this profile does not
    /// write, are refused.
    #[test]
    fn a_time_reads_back_as_the_second_it_states() {
        for unix in [
            0,
            951_782_400,
            1_792_413_296,
            2_524_607_999,
            2_524_608_000,
            4_107_542_400,
            253_402_300_799,
        ] {
            let mut out = Vec::new();
            encode(&mut out, unix).unwrap();
            let mut input = out.as_slice();
            let read = der::next(&mut input).unwrap();
            assert_eq!(decode(&read).unwrap(), unix, "{unix}");
        }
        let read = |tag: u8, text: &str| {
            let mut out = Vec::new();
            der::tlv(&mut out, tag, text.as_bytes()).unwrap();
            let mut input = out.as_slice();
            decode(&der::next(&mut input).unwrap())
        };
        // 1969-12-31T23:59:59Z, before the epoch, states 0.
        assert_eq!(read(der::UTC_TIME, "691231235959Z").unwrap(), 0);
        for (tag, text) in [
            (der::UTC_TIME, "260230000000Z"),
            (der::UTC_TIME, "250229000000Z"),
            (der::UTC_TIME, "261301000000Z"),
            (der::UTC_TIME, "261019240000Z"),
            (der::UTC_TIME, "2610191234Z"),
            (der::UTC_TIME, "261019123456"),
            (der::UTC_TIME, "26101912345+Z"),
            (der::GENERALIZED_TIME, "21000229000000Z"),
            (der::GENERALIZED_TIME, "20501019123456.5Z"),
            (der::OCTET_STRING, "261019123456Z"),
        ] {
            assert!(read(tag, text).is_err(), "{text}");
        }
        // 2000 and 2400 are leap years; 2100 is not.
        assert!(read(der::UTC_TIME, "000229000000Z").is_ok());
        assert!(read(der::GENERALIZED_TIME, "24000229000000Z").is_ok());
    }
}
