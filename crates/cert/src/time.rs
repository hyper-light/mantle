//! A certificate's times (RFC 5280 §4.1.2.5): UTCTime `YYMMDDHHMMSSZ` through 2049 and
//! GeneralizedTime `YYYYMMDDHHMMSSZ` from 2050, both in UTC with seconds and no fraction, from
//! seconds since the Unix epoch. The civil date is Hinnant's `civil_from_days` ("chrono-Compatible
//! Low-Level Date Algorithms", proleptic Gregorian calendar), over non-negative days only.

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
}
