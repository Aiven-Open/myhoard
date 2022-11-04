Name:           myhoard
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            https://github.com/aiven/myhoard
Summary:        MySQL streaming backup service
BuildArch:      noarch
License:        ASL 2.0
Source0:        myhoard-rpm-src.tar
BuildRequires:  percona-xtrabackup-80 >= 8.0
BuildRequires:  python3-aiohttp
BuildRequires:  python3-devel
BuildRequires:  python3-flake8
BuildRequires:  python3-httplib2
BuildRequires:  python3-isort
BuildRequires:  python3-pylint
BuildRequires:  python3-PyMySQL >= 0.9.2
BuildRequires:  python3-pytest
BuildRequires:  python3-pytest-cov
BuildRequires:  python3-requests
BuildRequires:  python3-rohmu
BuildRequires:  python3-socks
BuildRequires:  python3-yapf
BuildRequires:  rpm-build
BuildRequires:  systemd-rpm-macros
Requires:       percona-xtrabackup-80 >= 8.0
Requires:       python3-aiohttp
Requires:       python3-cryptography >= 0.8
Requires:       python3-PyMySQL >= 0.9.2
Requires:       python3-rohmu >= 1.0.7
Requires:       systemd

%undefine _missing_build_ids_terminate_build

%description
MyHoard is a MySQL streaming backup service.  Backups are stored in
encrypted and compressed format in a cloud object storage.  MyHoard
currently supports Amazon Web Services S3, Google Cloud Storage and
Microsoft Azure.


%global debug_package %{nil}


%prep
%setup -q -n myhoard


%build


%install
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 myhoard.unit %{buildroot}%{_unitdir}/myhoard.service


%check


%files
%defattr(-,root,root,-)
%doc LICENSE README.md myhoard.json
%{_bindir}/myhoard*
%{_unitdir}/myhoard.service
%{python3_sitelib}/*


%changelog
* Mon May 27 2019 Rauli Ikonen <rauli@aiven.io> - 1.0.0
- Initial version
