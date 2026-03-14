Name:           falcondb
Version:        %{_version}
Release:        1%{?dist}
Summary:        High-performance distributed SQL database

License:        Proprietary
URL:            https://github.com/falcondb/falcondb

AutoReqProv:    no
Requires:       glibc >= 2.17
Requires(pre):  shadow-utils

%description
FalconDB is a PostgreSQL wire-compatible distributed database engine
designed for high throughput, low latency OLTP workloads with strong
consistency guarantees.

Features:
 - PostgreSQL wire protocol (psql, JDBC, any PG driver)
 - MVCC with snapshot isolation and serializable modes
 - Automatic sharding with cross-shard transactions
 - WAL-based crash recovery and streaming replication
 - Built-in connection pooling and prepared statement caching

%pre
# Create falcondb system user if not exists
getent group falcondb >/dev/null || groupadd -r falcondb
getent passwd falcondb >/dev/null || \
    useradd -r -g falcondb -d /var/lib/falcondb -s /sbin/nologin \
    -c "FalconDB Database Server" falcondb
exit 0

%install
rm -rf %{buildroot}

# Binaries
install -D -m 0755 %{_sourcedir}/falcon     %{buildroot}/usr/bin/falcon
install -D -m 0755 %{_sourcedir}/falcon-cli  %{buildroot}/usr/bin/falcon-cli

# Config
install -D -m 0640 %{_sourcedir}/falcon.toml %{buildroot}/etc/falcondb/falcon.toml

# systemd unit
install -D -m 0644 %{_sourcedir}/falcondb.service %{buildroot}/usr/lib/systemd/system/falcondb.service

# Directories
install -d -m 0750 %{buildroot}/var/lib/falcondb
install -d -m 0750 %{buildroot}/var/log/falcondb

%files
%attr(0755, root, root) /usr/bin/falcon
%attr(0755, root, root) /usr/bin/falcon-cli
%config(noreplace) %attr(0640, root, falcondb) /etc/falcondb/falcon.toml
%attr(0644, root, root) /usr/lib/systemd/system/falcondb.service
%dir %attr(0750, falcondb, falcondb) /var/lib/falcondb
%dir %attr(0750, falcondb, falcondb) /var/log/falcondb

%post
%systemd_post falcondb.service
echo "FalconDB installed. Start with: sudo systemctl start falcondb"

%preun
%systemd_preun falcondb.service

%postun
%systemd_postun_with_restart falcondb.service
# Do not remove user/data on uninstall — data preservation

%changelog
