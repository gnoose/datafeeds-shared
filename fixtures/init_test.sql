create user gridium;
alter user gridium with encrypted password 'gridium';

grant all privileges on database gridium_test to gridium;
alter user gridium with superuser;
