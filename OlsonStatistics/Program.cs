using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace OlsonStatistics
{
    class Program
    {
        public const char COMMENT_CHAR = '#';
        private static Regex _regex;

        static void Main(string[] args)
        {
            // Directory input.
            string olsonDirectory = args.Length >= 1 ? args[0] : null;
            if (string.IsNullOrWhiteSpace(olsonDirectory) || !Directory.Exists(olsonDirectory))
            {
                Console.WriteLine("Please enter the Olson directory path...");
                olsonDirectory = Console.ReadLine();
                while (string.IsNullOrWhiteSpace(olsonDirectory) && !Directory.Exists(olsonDirectory))
                {
                    Console.WriteLine("Invalid Directory entered.");
                    Console.WriteLine("Please enter a valid Olson Directory path...");
                    olsonDirectory = Console.ReadLine();
                }
            }
            Console.WriteLine();


            // Output file/io input.
            string outputFile = args.Length >= 3 ? args[1] : null;
            if (string.IsNullOrWhiteSpace(outputFile))
            {
                Console.WriteLine("Please enter the output file path (enter nothing for none)...");
                outputFile = Console.ReadLine();
                while (string.IsNullOrWhiteSpace(outputFile))
                {
                    Console.WriteLine("Invalid path entered.");
                    Console.WriteLine("Please enter a valid output file path (or nothing for just console output)...");
                    outputFile = Console.ReadLine();
                }
            }
            Console.WriteLine();

            // Begin processing.
            Console.WriteLine("Processing Olson database statistics, please wait...");

            var timeZones = new List<object[]>();
            var rules = new List<object[]>();
            var links = new List<object[]>();

            FileInfo[] olsonFiles = new DirectoryInfo(olsonDirectory).GetFiles();

            string[] validFiles = new string[] { "africa", "antartica", "asia", "australasia", "europe", "northamerica", "southamerica" };

            FileInfo isoFile = new FileInfo(olsonDirectory + @"\iso3166.tab");
            FileInfo zoneFile = new FileInfo(olsonDirectory + @"\zone.tab");

            int curYear = DateTime.Now.Year;
            int lastUID = 0;

            _regex = new Regex(@"\s+", RegexOptions.Compiled);

            // Map that will store Country Codes to Country Names from iso3166.
            var ccToCn = new Dictionary<string, string>(275);
            // Map that will store TZ names to id, country codes, country names, coordinates, and comments.
            var tzNameToInfo = new Dictionary<string, string[]>(450);

            int totalEntities = 0;
            int totalComments = 0;
            int totalLines = 0;

            using (var input = new StreamReader(isoFile.FullName))
            {
                while (!input.EndOfStream)
                {
                    string line = input.ReadLine();
                    totalLines++;

                    if (!isCommentNullOrWhiteSpace(line))
                    {
                        // Parse countryCode and countryName from each line.
                        string countryCode = line.Substring(0, 2);
                        string countryName = line.Substring(3, line.Length - 3);

                        ccToCn.Add(countryCode, countryName);
                        totalEntities++;
                    }
                    else
                    {
                        totalComments++;
                    }
                }
            }

            using (var input = new StreamReader(zoneFile.FullName))
            {
                // Unique ID assigned to each TZName.
                lastUID = 0;
                while (!input.EndOfStream)
                {
                    string line = input.ReadLine();
                    totalLines++;
                    if (!isCommentNullOrWhiteSpace(line))
                    {
                        string[] fields = _regex.Split(line);

                        // Parsing fields.
                        string cCode = fields[0];
                        string coord = fields[1];
                        string tzName = fields[2];
                        string comments = null;
                        if (fields.Length > 3)
                        {
                            comments = fields[3];
                        }
                        string cName = "";
                        string uIdStr = (++lastUID).ToString();
                        if (!ccToCn.TryGetValue(cCode, out cName))
                        {
                        }

                        tzNameToInfo.Add(tzName, new string[] { uIdStr, cCode, cName, coord, comments });
                        totalEntities++;
                    }
                    else
                    {
                        totalComments++;
                    }
                }
            }

            var timeZoneLookupTable = new Dictionary<string, object[]>();

            foreach (FileInfo file in olsonFiles)
            {
                if (validFiles.Contains(file.Name))
                {
                    string[] lines = File.ReadAllLines(file.FullName);
                    totalLines += lines.Length;

                    // Process items.
                    for (int i = 0; i < lines.Length; i++)
                    {
                        string line = lines[i].TrimStart();

                        if (!isCommentNullOrWhiteSpace(line))
                        {
                            string[] fields = ParseFields(line);

                            totalEntities++;

                            switch (fields[0])
                            {
                                case Rule.RULE_NAME:
                                    {
                                        short from = Rule.ParseStartYear(fields[Rule.FromIndex]);
                                        short to = Rule.ParseEndYear(fields[Rule.ToIndex], from);

                                        if (from > curYear || to < curYear)
                                        {
                                            continue;
                                        }

                                        // No parsing necessary.
                                        string name = fields[Rule.NameIndex];

                                        var rule = new object[]
                                            {
                                                DBNull.Value,
                                                name,
                                                Rule.ParseBias(fields[Rule.SaveIndex]),
                                                from,
                                                to,
                                                Rule.ParseMonth(fields[Rule.InIndex]),
                                                fields[Rule.OnIndex],
                                                Rule.ParseTime(fields[Rule.AtIndex]),
                                                Rule.ParseTimeType(ref fields[Rule.AtIndex]),
                                                Rule.ParseAbrev(fields[Rule.LetterIndex]) ?? (object)System.DBNull.Value
                                            };

                                        rules.Add(rule);
                                        break;
                                    }

                                case TimeZone.ZONE_NAME:
                                    {
                                        // First TZ.
                                        int init = i;
                                        // Count to last
                                        do
                                        {
                                            i++;
                                        } while (i < lines.Length && TimeZone.IsContinuation(lines[i]));

                                        // If first was not the only...
                                        if (i - init != 1)
                                        {
                                            string[] continuation = ParseFields(lines[--i].Trim());

                                            // Continuation has a non-tab delimited date...
                                            if (continuation.Length > 4 && int.Parse(continuation[3]) < curYear)
                                            {
                                                continue;
                                            }
                                            System.Array.Copy(continuation, 0, fields, 2, continuation.Length);
                                        }
                                        else
                                        {
                                            if (fields.Length > 5 && int.Parse(fields[5]) < curYear)
                                            {
                                                continue;
                                            }
                                        }

                                        string name = TimeZone.ParseName(fields[TimeZone.NameIndex]);
                                        string countryCode = null;
                                        string countryName = null;
                                        string coord = null;
                                        string comments = null;
                                        int id = 0;
                                        string[] tokens;
                                        if (tzNameToInfo.TryGetValue(name, out tokens))
                                        {
                                            id = System.Convert.ToInt32(tokens[0]);
                                            countryCode = tokens[1];
                                            countryName = tokens[2];
                                            coord = tokens[3];
                                            comments = tokens[4];
                                        }
                                        short bias = TimeZone.ParseBias(fields[TimeZone.GMTOffsetIndex]);
                                        string ruleName = TimeZone.ParseRuleName(fields[TimeZone.RuleIndex]);
                                        string tzAbrev = TimeZone.ParseTzAbrev(fields[TimeZone.FormatIndex]);

                                        object[] timeZone = new object[]
                                            {
                                                id,
                                                name,
                                                bias,
                                                ruleName,
                                                tzAbrev,
                                                countryCode,
                                                countryName,
                                                comments,
                                                coord
                                            };

                                        timeZoneLookupTable[name] = timeZone;
                                        timeZones.Add(timeZone);
                                        break;
                                    }
                                case Link.LINK_NAME:
                                    {
                                        object[] link = new object[] { fields[Link.FromZoneNameIndex], fields[Link.ToZoneNameIndex] };
                                        links.Add(link);
                                        break;
                                    }
                                default:
                                    {
                                        continue;
                                    }
                            }
                        }
                        else
                        {
                            totalComments++;
                        }
                    }
                }
            }
            // Adding all links to list of TimeZones.
            foreach (object[] link in links)
            {
                object[] from;
                if (!timeZoneLookupTable.TryGetValue((string)link[Link.FromZoneNameIndex], out from))
                {
                    continue;
                }

                string toName = (string)link[Link.ToZoneNameIndex];
                object[] to = new object[]
                    {
                        ++lastUID,
                        toName,
                        from[2],
                        from[3],
                        from[4],
                        from[5],
                        from[6],
                        from[7],
                        from[8]
                    };
                timeZoneLookupTable.Add(toName, to);
                timeZones.Add(to);
                totalEntities++;
            }

            Console.WriteLine("\nDone!");

            StringBuilder sb = new StringBuilder("Results:\n");
            sb.AppendLine("There are " + totalEntities + " total entities (not necesarily parsed).");
            sb.AppendLine("There are " + totalComments + " comment lines.");
            sb.AppendLine("There are " + totalLines + " total lines.");
            sb.AppendLine("There are " + timeZones.Count + " timezones.");
            sb.AppendLine("There are " + rules.Count + " rules.");
            sb.AppendLine("There are " + links.Count + " links.");
            sb.AppendLine("For a total of" + timeZones.Count + rules.Count + links.Count + " parsed entities.");
            sb.AppendLine(string.Empty);

            string result = sb.ToString();
            Console.WriteLine(result);

            if (null != outputFile)
            {
                using (StreamWriter output = new StreamWriter(outputFile, false))
                {
                    output.WriteLine(result);
                }
                Console.WriteLine("Output successfully written.");
            }
            Console.WriteLine("Press any key to terminate...");
            Console.ReadLine();
        }

        /// <summary>
        /// Returns whether the provided line is a comment or empty line.
        /// </summary>
        /// <param name="line">The line to determine.</param>
        /// <returns>True if the provided line is a comment or empty, false otherwise.</returns>
        private static bool isCommentNullOrWhiteSpace(string line)
        {
            return string.IsNullOrWhiteSpace(line) || line[0] == COMMENT_CHAR;
        }

        /// <summary>
        /// Parses the provided line into fields.
        /// </summary>
        /// <param name="line">The line to parse.</param>
        /// <returns>The fields parsed from the provided line.</returns>
        private static string[] ParseFields(string line)
        {
            // Eliminating inline comments.
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (c == COMMENT_CHAR)
                {
                    break;
                }
                sb.Append(c);
            }
            return _regex.Split(sb.ToString());
        }
    }
}
