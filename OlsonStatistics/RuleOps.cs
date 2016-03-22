/* (c) Copyright Francisco Aguilera (Falven)
 * You are free to edit and distribute this
 * source so long as this statement remains
 * in place, here and in all other such files.
 */

using System;
using System.Data;
using System.Data.Linq;

namespace OlsonStatistics
{
    /// <summary>
    /// Static methods and constant integers representing various parsing operations
    /// to be done on a Rule to extract property information, and the field indexes
    /// of such properties, consequently.
    /// 
    /// Fields:
    /// Rule NAME    FROM    TO  TYPE    IN  ON  AT  SAVE    LETTER/S
    /// Sample:
    /// Rule	US	2007	max	-	Nov	Sun>=1	2:00	0	S
    /// </summary>
    partial class Rule
    {
        public const string TABLE_SCHEMA = "dbo";
        public const string TABLE_NAME = "rules";
        public const string FULL_TABLE_NAME = TABLE_SCHEMA + "." + TABLE_NAME;

        public const string RULE_NAME = "Rule";
        public const int MAX_YEAR = 32767;
        public const int NameIndex = 1;
        public const int FromIndex = 2;
        public const int ToIndex = 3;
        public const int TypeIndex = 4;
        public const int InIndex = 5;
        public const int OnIndex = 6;
        public const int AtIndex = 7;
        public const int SaveIndex = 8;
        public const int LetterIndex = 9;

        /// <summary>
        /// Parses the year on which this rule begins taking place. for this rule.
        /// </summary>
        /// <param name="startYear">The string to parse into the start year for this rule.</param>
        /// <returns>The start year for this rule.</returns>
        internal static short ParseStartYear(string startYear)
        {
            // Any integer year can be supplied; the Gregorian calendar is assumed.
            // The word minimum (or an abbreviation) means the minimum year representable as an integer.
            // The word maximum (or an abbreviation) means the maximum year representable as an integer.
            // Rules can describe times that are not representable as time values, with the unrepresentable times ignored;
            // this allows rules to be portable among hosts with differing time value types.
            short from;
            if (startYear.StartsWith("max"))
            {
                from = MAX_YEAR;
            }
            else if (startYear.StartsWith("min"))
            {
                from = 0;
            }
            else
            {
                if (!short.TryParse(startYear, out from))
                {
                }
            }
            return from;
        }

        /// <summary>
        /// Parses the year on which this rule no longer applies.
        /// </summary>
        /// <param name="p">The string to parse into the end year.</param>
        /// <returns>The end year for this rule.</returns>
        internal static short ParseEndYear(string endYear, short startYear)
        {
            // In addition to minimum and maximum (as above), the word only (or an abbreviation)
            // may be used to repeat the value of the FROM field.
            short to;
            if (endYear.StartsWith("only"))
            {
                to = startYear;
            }
            else
            {
                to = ParseStartYear(endYear);
            }
            return to;
        }

        /// <summary>
        /// Parses the month in which this rule applies.
        /// </summary>
        /// <param name="p">The string to parse into the month for this rule.</param>
        /// <returns>The month for this rule.</returns>
        internal static byte ParseMonth(string month)
        {
            // Month names may be abbreviated.
            month = month.Substring(0, 3).ToLower();
            switch (month)
            {
                case "jan":
                    return 1;
                case "feb":
                    return 2;
                case "mar":
                    return 3;
                case "apr":
                    return 4;
                case "may":
                    return 5;
                case "jun":
                    return 6;
                case "jul":
                    return 7;
                case "aug":
                    return 8;
                case "sep":
                    return 9;
                case "oct":
                    return 10;
                case "nov":
                    return 11;
                case "dec":
                    return 12;
                default:
                    {
                        return 0;
                    }
            }
        }

        /// <summary>
        /// Parses the provided string into a Date representing the effective date of this rule.
        /// </summary>
        /// <param name="on">The field representing the date on whioh the rule takes effect.</param>
        /// <returns>The Date representation of the provided fields.</returns>
        public static string ParseDate(string date)
        {
            // Recognized forms:
            //      5 the fifth of the month
            //      lastSun  the last Sunday in the month
            //      lastMon  the last Monday in the month
            //      Sun>=8   first Sunday on or after the eighth
            //      Sun<=25  last Sunday on or before the 25th
            // Names of days of the week may be abbreviated or spelled out in full.
            // Note that there must be no spaces within the ON field.

            // Need to somehow look at calendar and return calendar day of this date...
            return date.ToLower();
        }

        /// <summary>
        /// Parses the time at which this rule applies.
        /// </summary>
        /// <param name="p">The string to parse into the time for this rule.</param>
        /// <returns>The time for this rule.</returns>
        internal static TimeSpan ParseTime(string time)
        {
            // Recognized forms:
            //      2        time in hours
            //      2:00     time in hours and minutes
            //      15:00    24-hour format time (for times after noon)
            //      1:28:14  time in hours, minutes, and seconds
            //      -        equivalent to 0
            // where hour 0 is midnight at the start of the day,
            // and hour 24 is midnight at the end of the day.
            TimeSpan result;
            if (time.StartsWith("24"))
            {
                result = new TimeSpan(0, 23, 59, 59, 999);
            }
            else
            {
                if (!TimeSpan.TryParse(time, out result))
                {
                }
            }
            return result;
        }

        /// <summary>
        /// Parses the type of time of the provided string.
        /// </summary>
        /// <param name="time">The string to parse the type of.</param>
        /// <returns>The type of time, eg: local standard time, wall clock time, or universal time.</returns>
        internal static char ParseTimeType(ref string time)
        {
            // Any of these forms may be followed by the letter w if
            // the given time is local "wall clock" time, s if the
            // given time is local "standard" time, or u (or g or
            // z) if the given time is universal time; in the
            // absence of an indicator, wall clock time is assumed.

            // Defaults to w.
            char type = 'w';
            int last = time.Length - 1;
            char lastChar = time[last];
            if (!char.IsDigit(lastChar))
            {
                time = time.Substring(0, last);
                type = lastChar;
            }
            return type;
        }

        /// <summary>
        /// Parses the "variable part" (for example, the "S" or "D" in "EST" or "EDT")
        /// of time zone abbreviations to be used when this rule is in effect.  If this field
        /// is -, the variable part is null.
        /// </summary>
        /// <param name="p">The string to parse into the type for this rule.</param>
        /// <returns>The type for this rule.</returns>
        public static char? ParseAbrev(string abrev)
        {
            char letter;
            if (abrev[0] == '-')
            {
                return null;
            }
            if (!char.TryParse(abrev, out letter))
            {
                return null;
            }
            return letter;
        }

        /// <summary>
        /// Parses the amount of time to be added to local standard time when 
        /// the rule is in effect. This field has the same format as the AT
        /// field (although, of course, the w and s suffixes are not used).
        /// </summary>
        /// <param name="p">The string to parse into a rule's bias.</param>
        /// <returns>The bias parsed from the provided string.</returns>
        public static short ParseBias(string bias)
        {
            // Recognized forms:
            //      2        time in hours
            //      2:00     time in hours and minutes
            //      15:00    24-hour format time (for times after noon)
            //      1:28:14  time in hours, minutes, and seconds
            //      -        equivalent to 0
            // where hour 0 is midnight at the start of the day,
            // and hour 24 is midnight at the end of the day.  Any
            // of these forms may be followed by the letter w if
            // the given time is local "wall clock" time, s if the
            // given time is local "standard" time, or u (or g or
            // z) if the given time is universal time; in the
            // absence of an indicator, wall clock time is assumed.
            return (short)(((TimeSpan)ParseTime(bias)).TotalSeconds);
        }
    }
}
