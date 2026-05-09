package com.orcp.flink;

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads SQL scripts from the classpath, substitutes {@code ${var}} placeholders
 * against a {@link ParameterTool}, splits them into statements and executes
 * them on a {@link TableEnvironment}.
 *
 * <p>Behaviour contract:
 * <ul>
 *   <li>DDL statements (CREATE TABLE / CREATE VIEW / SET / USE) are executed
 *       immediately via {@link TableEnvironment#executeSql(String)}.</li>
 *   <li>INSERT statements are collected into a {@link StatementSet} and
 *       executed together at the end as a single Flink job.  That lets the
 *       pipeline file contain multiple INSERTs if needed without spawning
 *       independent jobs.</li>
 *   <li>Line comments start with {@code --}; block comments {@code / * ... * /}
 *       are also stripped.  Empty trailing semicolons are ignored.</li>
 *   <li>Unresolved placeholders raise {@link IllegalStateException} with the
 *       offending name -- silent substitution-failure would be very hard to
 *       debug in production.</li>
 * </ul>
 *
 * <p>Deliberately dependency-free at the JDK layer: no {@code Files.readString}
 * (JDK 11+) -- we stick with commons-io so the job jar still works on JDK 8.
 */
public final class SqlLoader {

    private static final Logger LOG = LoggerFactory.getLogger(SqlLoader.class);

    /** {@code ${name}} or {@code ${name.with.dots}}; alphanumerics, dot, dash, underscore. */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_.\\-]+)}");

    private SqlLoader() {
    }

    /**
     * Loads and executes the listed classpath resources against {@code tEnv}.
     * The files are processed in the order given; the first INSERT encountered
     * anywhere forms part of a single {@link StatementSet} that is executed
     * once after all files are parsed.
     *
     * @return the Flink {@link TableResult} for the combined INSERT set, or
     *         {@code null} if there were no INSERTs to run (pure DDL).
     */
    public static TableResult loadAndExecute(TableEnvironment tEnv,
                                             ParameterTool params,
                                             String... classpathResources) {
        StatementSet inserts = tEnv.createStatementSet();
        int insertCount = 0;

        for (String resource : classpathResources) {
            LOG.info("Loading SQL resource {}", resource);
            String raw = readResource(resource);
            String resolved = substitute(raw, params);
            List<String> statements = splitStatements(resolved);
            LOG.info("  resource {} -> {} statement(s)", resource, statements.size());

            for (String stmt : statements) {
                String trimmed = stmt.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (isInsert(trimmed)) {
                    LOG.info("  enqueuing INSERT ({} chars) into StatementSet", trimmed.length());
                    inserts.addInsertSql(trimmed);
                    insertCount++;
                } else {
                    LOG.info("  executing DDL: {}", shortHead(trimmed));
                    tEnv.executeSql(trimmed);
                }
            }
        }

        if (insertCount == 0) {
            LOG.warn("No INSERT statements found across {} resource(s); nothing to run.",
                    classpathResources.length);
            return null;
        }

        LOG.info("Submitting StatementSet with {} INSERT(s)", insertCount);
        return inserts.execute();
    }

    // -- helpers ----------------------------------------------------------

    static String readResource(String path) {
        try (InputStream in = SqlLoader.class.getResourceAsStream(path)) {
            Objects.requireNonNull(in, "classpath resource not found: " + path);
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read " + path, e);
        }
    }

    /**
     * Resolves every {@code ${name}} placeholder by looking it up in
     * {@code params}.  Throws on a missing key rather than leaving the
     * placeholder literal in the SQL, which would manifest downstream as
     * a cryptic ValidationException.
     */
    static String substitute(String sql, ParameterTool params) {
        Matcher m = PLACEHOLDER.matcher(sql);
        StringBuffer out = new StringBuffer(sql.length());
        while (m.find()) {
            String key = m.group(1);
            if (!params.has(key)) {
                throw new IllegalStateException(
                        "Missing required SQL parameter '" + key + "'; "
                                + "declare it in job.properties or pass --" + key + " <value>");
            }
            // Matcher.quoteReplacement so values containing $ or \ survive intact.
            m.appendReplacement(out, Matcher.quoteReplacement(params.get(key)));
        }
        m.appendTail(out);
        return out.toString();
    }

    /**
     * Splits a block of SQL into individual statements.  Comments are
     * stripped first so a {@code ;} inside a line-comment doesn't trigger
     * a spurious break.  A final statement without a trailing semicolon
     * is accepted.
     */
    static List<String> splitStatements(String sql) {
        // Remove /* ... */ block comments (non-greedy, dot-matches-newline).
        String noBlock = sql.replaceAll("(?s)/\\*.*?\\*/", "");
        // Remove -- line comments (rest of line).
        StringBuilder clean = new StringBuilder(noBlock.length());
        for (String line : noBlock.split("\n", -1)) {
            int dash = line.indexOf("--");
            clean.append(dash < 0 ? line : line.substring(0, dash));
            clean.append('\n');
        }
        List<String> out = new ArrayList<>();
        for (String piece : clean.toString().split(";")) {
            String t = piece.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        return out;
    }

    static boolean isInsert(String stmt) {
        // match "INSERT" as the first keyword, case-insensitive.  We need a
        // word boundary after it so identifiers like "INSERTING" or
        // "INSERTED_AT" don't accidentally classify as INSERT statements.
        // regionMatches alone would accept any prefix, which is wrong here.
        String head = stmt.replaceFirst("^\\s+", "");
        if (head.length() < 7) {
            return false;
        }
        if (!head.regionMatches(true, 0, "INSERT", 0, 6)) {
            return false;
        }
        return Character.isWhitespace(head.charAt(6));
    }

    private static String shortHead(String stmt) {
        String s = stmt.replaceAll("\\s+", " ");
        return s.length() <= 120 ? s : s.substring(0, 120) + "...";
    }
}
