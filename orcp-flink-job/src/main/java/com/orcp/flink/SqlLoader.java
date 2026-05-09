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
 * 从 classpath 读取 SQL 脚本，将 {@code ${var}} 占位符替换为 {@link ParameterTool} 中的值，
 * 将语句拆分后在 {@link TableEnvironment} 上执行。
 *
 * <p>行为契约：
 * <ul>
 *   <li>DDL 语句（CREATE TABLE / CREATE VIEW / SET / USE）通过
 *       {@link TableEnvironment#executeSql(String)} 立即执行。</li>
 *   <li>INSERT 语句收集到 {@link StatementSet} 中，最终合并为一个 Flink 作业提交。
 *       这样流水线文件可以包含多个 INSERT 而不会创建多个独立作业。</li>
 *   <li>行注释以 {@code --} 开头；块注释 {@code / * ... * /} 也会被清除。
 *       空的尾部分号会被忽略。</li>
 *   <li>未解析的占位符会抛出 {@link IllegalStateException} 并包含缺失的键名——
 *       静默替换失败在生产环境中极难调试。</li>
 * </ul>
 *
 * <p>刻意不使用 JDK 层面的高级 API：不使用 {@code Files.readString}（JDK 11+），
 * 而使用 commons-io，以确保 job jar 在 JDK 8 上正常运行。
 */
public final class SqlLoader {

    private static final Logger LOG = LoggerFactory.getLogger(SqlLoader.class);

    /** 匹配 {@code ${name}} 或 {@code ${name.with.dots}}；字母数字、点、横线、下划线。 */
    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([A-Za-z0-9_.\\-]+)}");

    private SqlLoader() {
    }

    /**
     * 按顺序加载并执行 classpath 资源中的 SQL 语句。
     * 所有文件中遇到的第一个 INSERT 语句均纳入同一 {@link StatementSet}，
     * 全部文件解析完毕后统一提交。
     *
     * @return 合并 INSERT 集合的 {@link TableResult}，若无 INSERT 则返回 {@code null}。
     */
    public static TableResult loadAndExecute(TableEnvironment tEnv,
                                             ParameterTool params,
                                             String... classpathResources) {
        StatementSet inserts = tEnv.createStatementSet();
        int insertCount = 0;

        for (String resource : classpathResources) {
            LOG.info("加载 SQL 资源：{}", resource);
            String raw = readResource(resource);
            String resolved = substitute(raw, params);
            List<String> statements = splitStatements(resolved);
            LOG.info("  资源 {} -> {} 条语句", resource, statements.size());

            for (String stmt : statements) {
                String trimmed = stmt.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                if (isInsert(trimmed)) {
                    LOG.info("  将 INSERT（{} 字符）加入 StatementSet", trimmed.length());
                    inserts.addInsertSql(trimmed);
                    insertCount++;
                } else {
                    LOG.info("  执行 DDL：{}", shortHead(trimmed));
                    tEnv.executeSql(trimmed);
                }
            }
        }

        if (insertCount == 0) {
            LOG.warn("在 {} 个资源中未找到 INSERT 语句，无作业可提交。",
                    classpathResources.length);
            return null;
        }

        LOG.info("提交 StatementSet，共 {} 条 INSERT", insertCount);
        return inserts.execute();
    }

    // -- 辅助方法 ----------------------------------------------------------

    static String readResource(String path) {
        try (InputStream in = SqlLoader.class.getResourceAsStream(path)) {
            Objects.requireNonNull(in, "classpath 资源不存在：" + path);
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("读取 " + path + " 失败", e);
        }
    }

    /**
     * 将 SQL 中每个 {@code ${name}} 占位符替换为 {@code params} 中对应的值。
     * 若键缺失则抛出异常（而非静默保留占位符）——占位符原样保留会导致下游
     * 产生难以排查的 ValidationException。
     */
    static String substitute(String sql, ParameterTool params) {
        Matcher m = PLACEHOLDER.matcher(sql);
        StringBuffer out = new StringBuffer(sql.length());
        while (m.find()) {
            String key = m.group(1);
            if (!params.has(key)) {
                throw new IllegalStateException(
                        "缺少必要的 SQL 参数 '" + key + "'；"
                                + "请在 job.properties 中声明或通过 --" + key + " <value> 传入");
            }
            // Matcher.quoteReplacement 确保值中包含 $ 或 \ 时不会被误解析。
            m.appendReplacement(out, Matcher.quoteReplacement(params.get(key)));
        }
        m.appendTail(out);
        return out.toString();
    }

    /**
     * 将 SQL 块拆分为单条语句。先清除注释（防止注释中的 {@code ;} 触发误拆分），
     * 再按 {@code ;} 分割。不带尾部分号的最后一条语句也会被接受。
     */
    static List<String> splitStatements(String sql) {
        // 清除 /* ... */ 块注释（非贪婪，支持跨行）。
        String noBlock = sql.replaceAll("(?s)/\\*.*?\\*/", "");
        // 清除 -- 行注释。
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

    /**
     * 判断语句是否为 INSERT（大小写不敏感），
     * 需确保 INSERT 后有空白字符，避免将 INSERTING 等标识符误判。
     */
    static boolean isInsert(String stmt) {
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
