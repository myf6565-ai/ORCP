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
 * Minimum viable auth for the control plane (DEV_SPEC §6.4):
 *
 * <ul>
 *   <li>Basic auth over a single in-memory admin user; credentials are
 *       resolved from {@code orcp.admin.username}/{@code orcp.admin.password}
 *       (real values injected via Nacos).</li>
 *   <li>Actuator {@code /actuator/health/**} and {@code /actuator/prometheus}
 *       are anonymous so Prometheus + systemd-level probes work without a
 *       credential.</li>
 *   <li>Everything else under {@code /api/**} and {@code /actuator/**}
 *       requires authentication.</li>
 *   <li>CSRF is disabled because the API is JSON + machine-to-machine.</li>
 * </ul>
 *
 * <p>No role hierarchy: we have one user and no intention of adding more
 * in the minimum deployment.  When the admin surface grows, upgrade to a
 * proper identity provider.
 */
@Configuration
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(sm -> sm.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // Anonymous health surface for Prometheus / systemd.
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
