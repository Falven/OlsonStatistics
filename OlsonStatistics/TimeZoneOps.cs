/* (c) Copyright Francisco Aguilera (Falven)
 * You are free to edit and distribute this
 * source so long as this statement remains
 * in place, here and in all other such files.
 */

using System;
using System.Data;
using System.Data.Linq;
using System.Text;

namespace OlsonStatistics
{
    /// <summary>
    /// Zone NAME    GMTOFF  RULES/SAVE  FORMAT  [UNTILYEAR [MONTH [DAY [TIME]]]]
    /// </summary>
    partial class TimeZone
    {
        public const string TABLE_SCHEMA = "dbo";
        public const string TABLE_NAME = "timezones";
        public const string FULL_TABLE_NAME = TABLE_SCHEMA + "." + TABLE_NAME;

        public const string ZONE_NAME = "Zone";
        public const int NameIndex = 1;
        public const int GMTOffsetIndex = 2;
        public const int RuleIndex = 3;
        public const int FormatIndex = 4;
        public const int ContGMTOffsetIndex = 0;
        public const int ContRulesIndex = 1;
        public const int ContFormatIndex = 2;
        public const int ContUntilIndex = 3;

        /// <summary>
        /// Whether the provided fields correspond to a timezone continuation line.
        /// </summary>
        /// <param name="line">The line to check for a continuation.</param>
        /// <returns>True if the line provided is a continuation, false otherwise.</returns>
        public static bool IsContinuation(string line)
        {
            if (line.Equals(string.Empty))
            {
                return false;
            }
            else if (line[0] == Program.COMMENT_CHAR)
            {
                return false;
            }
            else
            {
                line = line.Substring(0, 4);
                return !line.Equals(Rule.RULE_NAME) && !line.Equals(ZONE_NAME) && !line.Equals(Link.LINK_NAME) && !line.Equals("leap");
            }
        }

        /// <summary>
        /// Parses the provided string into a TimeZone's name.
        /// </summary>
        /// <param name="nameStr">The string to parse into a name.</param>
        /// <returns>The name for a TimeZone</returns>
        public static string ParseName(string nameStr)
        {
            return nameStr;
        }

        /// <summary>
        /// Parses the provided string into a TimeZone's bias.
        /// </summary>
        /// <param name="offset">The string to parse into a bias.</param>
        /// <returns>The bias parsed form the provided string.</returns>
        public static short ParseBias(string offset)
        {
            TimeSpan result;
            if (!TimeSpan.TryParse(offset, out result))
            {
            }
            return (short)result.TotalSeconds;
        }

        /// <summary>
        /// Parses the provided string into a TimeZone's rule name.
        /// </summary>
        /// <param name="name">The string to parse into a rule name.</param>
        /// <returns>The parsed rule name.</returns>
        public static string ParseRuleName(string name)
        {
            if (name[0] == '-')
            {
                return null;
            }
            return name;
        }

        /// <summary>
        /// Parses the provided TzAbrev into a TimeZone's abreviation.
        /// </summary>
        /// <param name="format">The string to parse into an abreviation.</param>
        /// <returns>The abreviation for the provided string.</returns>
        public static string ParseTzAbrev(string format)
        {
            return format;
        }
    }
}
