package com.orcp.admin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

/**
 * 控制面最小化认证配置（DEV_SPEC §6.4）：
 *
 * <ul>
 *   <li>单一内存 admin 用户 + Basic Auth；凭据从
 *       {@code orcp.admin.username}/{@code orcp.admin.password} 读取
 *       （真实值通过 Nacos 注入）。</li>
 *   <li>Actuator {@code /actuator/health/**} 和 {@code /actuator/prometheus}
 *       匿名可访问，供 Prometheus 和 systemd 健康探针使用。</li>
 *   <li>{@code /api/**} 和其余 {@code /actuator/**} 需要认证。</li>
 *   <li>CSRF 已禁用（纯 JSON 机器对机器 API）。</li>
 * </ul>
 *
 * <p>当前无角色层级：只有一个用户，最小化部署不需要复杂 RBAC。
 * 业务扩展后，建议接入正式 IdP。
 */
@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // 匿名放行：Prometheus 抓取和 systemd 健康探针无需凭据。
                        .antMatchers("/actuator/health/**",
                                     "/actuator/info",
                                     "/actuator/prometheus",
                                     "/api/health")
                        .permitAll()
                        .anyRequest().authenticated())
                .httpBasic(b -> {});
        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder encoder,
                                                 org.springframework.core.env.Environment env) {
        String username = env.getProperty("orcp.admin.username", "orcp-admin");
        String rawPassword = env.getProperty("orcp.admin.password", "ChangeMe_admin_1!");
        UserDetails user = User.withUsername(username)
                .password(encoder.encode(rawPassword))
                .roles("ADMIN")
                .build();
        return new InMemoryUserDetailsManager(user);
    }
}
