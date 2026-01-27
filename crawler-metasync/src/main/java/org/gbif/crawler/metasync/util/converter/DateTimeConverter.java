/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.crawler.metasync.util.converter;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.beanutils.Converter;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Used by commons-digester (via commons-beanutils) to convert Strings into {@link OffsetDateTime}.
 */
public class DateTimeConverter implements Converter {

  private static final Pattern WEEK_DATE_WITH_OPTIONAL_DAY =
    Pattern.compile("^(\\d{4})-?W(\\d{2})(?:-?(\\d))?$");

  @Override
  public Object convert(Class type, Object value) {
    checkNotNull(type, "type cannot be null");
    checkNotNull(value, "Value cannot be null");
    checkArgument(
      type.equals(OffsetDateTime.class), "Conversion target should be OffsetDateTime, but is %s", type);
    checkArgument(String.class.isAssignableFrom(value.getClass()), "Value should be a string, but is a %s", value.getClass());

    String s = ((String) value).trim();
    if (s.isEmpty()) {
      return null;
    }

    // Normalize common nuisances
    s = s.replace('\u2212', '-'); // replace unicode minus
    s = s.replaceAll("\\s+T\\s+", "T"); // sanitize stray spaces around T

    // 1) Try robust OffsetDateTime parsing using standard formatters and normalize to UTC
    try {
      OffsetDateTime odt = OffsetDateTime.parse(s, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
      return odt.withOffsetSameInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
    }
    try {
      OffsetDateTime odt = OffsetDateTime.parse(s, DateTimeFormatter.ISO_DATE_TIME);
      return odt.withOffsetSameInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
    }

    // 2) Handle basic Z and basic patterns like 20160127T061122Z or yyyyMMdd'T'HHmmssZ, normalize to UTC
    try {
      DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX").withLocale(Locale.ROOT);
      OffsetDateTime odt = OffsetDateTime.parse(s, f);
      return odt.withOffsetSameInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
    }

    // 3) Handle patterns with space as separator and offsets like +0300
    try {
      // e.g. "2016-01-27 09:11:22+0300"
      DateTimeFormatter f1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssZ").withLocale(Locale.ROOT);
      OffsetDateTime odt = OffsetDateTime.parse(s, f1);
      return odt.withOffsetSameInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
    }
    try {
      // e.g. "2016-01-27 09:11:22+03:00"
      DateTimeFormatter f2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX").withLocale(Locale.ROOT);
      OffsetDateTime odt = OffsetDateTime.parse(s, f2);
      return odt.withOffsetSameInstant(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
    }

    // 4) Time without offset -> assume UTC (tests expect this)
    try {
      DateTimeFormatter fNoZone1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withLocale(Locale.ROOT);
      LocalDateTime ldt = LocalDateTime.parse(s, fNoZone1);
      return ldt.atOffset(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
    }
    try {
      DateTimeFormatter fNoZone2 = DateTimeFormatter.ISO_LOCAL_DATE_TIME; // "yyyy-MM-dd'T'HH:mm:ss"
      LocalDateTime ldt = LocalDateTime.parse(s, fNoZone2);
      return ldt.atOffset(ZoneOffset.UTC);
    } catch (DateTimeParseException ignored) {
    }

    // 5) Week date handling: "2016-W04" or "2016-W04-3" (ISO week)
    Matcher m = WEEK_DATE_WITH_OPTIONAL_DAY.matcher(s);
    if (m.matches()) {
      int year = Integer.parseInt(m.group(1));
      int week = Integer.parseInt(m.group(2));
      String dayGroup = m.group(3);
      int dayOfWeek = (dayGroup == null) ? 1 : Integer.parseInt(dayGroup); // default to Monday
      // compute Monday of week-based-year and adjust to requested day
      LocalDate anchor = LocalDate.of(year, Month.JANUARY, 4); // ISO week-based year anchor
      LocalDate weekDate =
        anchor.with(WeekFields.ISO.weekOfWeekBasedYear(), (long) week)
          .with(WeekFields.ISO.dayOfWeek(), (long) dayOfWeek);
      return weekDate.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
    }

    // 6) Ordinal date: "yyyy-DDD" (e.g. 2016-027)
    try {
      LocalDate ld = LocalDate.parse(s, DateTimeFormatter.ISO_ORDINAL_DATE);
      return ld.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
    } catch (DateTimeParseException ignored) {
    }

    // 7) Basic ISO date: "yyyyMMdd"
    try {
      LocalDate ld = LocalDate.parse(s, DateTimeFormatter.BASIC_ISO_DATE);
      return ld.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
    } catch (DateTimeParseException ignored) {
    }

    // 8) Standard ISO date: "yyyy-MM-dd" and forgiving variants like "yyyy/MM/dd"
    String sDateCandidate = s.replace('/', '-');
    try {
      LocalDate ld = LocalDate.parse(sDateCandidate, DateTimeFormatter.ISO_DATE);
      return ld.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
    } catch (DateTimeParseException ignored) {
    }

    // 9) Partial dates: "yyyy-MM" -> first day of month, "yyyy" -> first day of year
    try {
      YearMonth ym = YearMonth.parse(sDateCandidate, DateTimeFormatter.ofPattern("yyyy-MM"));
      LocalDate ld = ym.atDay(1);
      return ld.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
    } catch (DateTimeParseException ignored) {
    }
    try {
      Year y = Year.parse(sDateCandidate, DateTimeFormatter.ofPattern("yyyy"));
      LocalDate ld = LocalDate.of(y.getValue(), 1, 1);
      return ld.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
    } catch (DateTimeParseException ignored) {
    }

    // 10) Last resort: if string starts with a date part, try extracting prefix date (handles weird suffixes)
    try {
      String prefix = sDateCandidate.split("[T\\s]")[0];
      LocalDate ld = LocalDate.parse(prefix, DateTimeFormatter.ISO_DATE);
      return ld.atStartOfDay(ZoneId.systemDefault()).toOffsetDateTime();
    } catch (Exception ignored) {
    }

    // Could not parse
    return null;
  }
}
