Summary: GRNOC TSDS Aggregate
Name: grnoc-tsds-aggregate
Version: 1.0.2
Release: 1%{?dist}
License: GRNOC
Group: Measurement
URL: http://globalnoc.iu.edu
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildArch: noarch
Requires: perl >= 5.8.8
Requires: perl-Try-Tiny
Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config
Requires: perl-Proc-Daemon
Requires: perl-List-MoreUtils
Requires: perl-MongoDB
Requires: perl-Net-AMQP-RabbitMQ
Requires: perl-JSON-XS
Requires: perl-Redis
Requires: perl-Redis-DistLock
Requires: perl-Time-HiRes
Requires: perl-Moo
Requires: perl-Types-XSD-Lite
Requires: perl-Parallel-ForkManager
Requires: perl-GRNOC-WebService-Client
Requires: perl-Math-Round

%description
GRNOC TSDS Aggregate Daemon and Workers

%prep
%setup -q -n grnoc-tsds-aggregate-%{version}

%build
%{__perl} Makefile.PL PREFIX="%{buildroot}%{_prefix}" INSTALLDIRS="vendor"
make

%install
rm -rf $RPM_BUILD_ROOT
make pure_install

%{__install} -d -p %{buildroot}/etc/grnoc/tsds/aggregate/
%{__install} -d -p %{buildroot}/var/lib/grnoc/tsds/aggregate/
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/init.d/
%{__install} -d -p %{buildroot}/usr/share/doc/grnoc/tsds-aggregate/

%{__install} CHANGES.md %{buildroot}/usr/share/doc/grnoc/tsds-aggregate/CHANGES.md
%{__install} INSTALL.md %{buildroot}/usr/share/doc/grnoc/tsds-aggregate/INSTALL.md

%{__install} conf/config.xml.example %{buildroot}/etc/grnoc/tsds/aggregate/config.xml
%{__install} conf/logging.conf.example %{buildroot}/etc/grnoc/tsds/aggregate/logging.conf

%{__install} init.d/tsds-aggregate-daemon %{buildroot}/etc/init.d/tsds-aggregate-daemon
%{__install} init.d/tsds-aggregate-workers %{buildroot}/etc/init.d/tsds-aggregate-workers

%{__install} bin/tsds-aggregate-daemon  %{buildroot}/usr/bin/tsds-aggregate-daemon
%{__install} bin/tsds-aggregate-workers %{buildroot}/usr/bin/tsds-aggregate-workers

# clean up buildroot
find %{buildroot} -name .packlist -exec %{__rm} {} \;

%{_fixperms} $RPM_BUILD_ROOT/*

%clean
rm -rf $RPM_BUILD_ROOT

%files

%defattr(640, root, root, -)

%config(noreplace) /etc/grnoc/tsds/aggregate/config.xml
%config(noreplace) /etc/grnoc/tsds/aggregate/logging.conf

%defattr(644, root, root, -)

/usr/share/doc/grnoc/tsds-aggregate/CHANGES.md
/usr/share/doc/grnoc/tsds-aggregate/INSTALL.md

%{perl_vendorlib}/GRNOC/TSDS/Aggregate.pm
%{perl_vendorlib}/GRNOC/TSDS/Aggregate/Daemon.pm
%{perl_vendorlib}/GRNOC/TSDS/Aggregate/Aggregator.pm
%{perl_vendorlib}/GRNOC/TSDS/Aggregate/Aggregator/Worker.pm
%{perl_vendorlib}/GRNOC/TSDS/Aggregate/Aggregator/Message.pm

%defattr(754, root, root, -)

/usr/bin/tsds-aggregate-daemon
/usr/bin/tsds-aggregate-workers

/etc/init.d/tsds-aggregate-daemon
/etc/init.d/tsds-aggregate-workers

%defattr(755, root, root, -)

%dir /var/lib/grnoc/tsds/aggregate/
