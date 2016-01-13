# Library Simplified Content Server

This is the Open Access Content Server for [Library Simplified](http://www.librarysimplified.org/). The oa content server collects and parses sources and preserves metadata for open access works, serving them up in a feed with verbose OPDS entries.

It depends on the [LS Server Core](https://github.com/NYPL/Simplified-server-core) as a git submodule.

## Installation

Thorough deployment instructions, including essential libraries for Linux systems, can be found [in the Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified-iOS/wiki/Deployment-Instructions). **_If this is your first time installing a Library Simplified server, please review those instructions._**

Keep in mind that the content server requires the Xvfb library, unique database names and a data directory, as detailed below.

### Xvfb library

Download the Xvfb library:
```sh
$ sudo yum install xorg-x11-server-Xvfb
```
Or, on Ubuntu:
```sh
$ sudo apt-get install xvfb
```

### Database

Create relevant databases in Postgres:
```sh
$ sudo -u postgres psql
CREATE DATABASE simplified_content_test;
CREATE DATABASE simplified_content_dev;

# Create users, unless you've already created them for another LS project
CREATE USER simplified with password '[password]';
CREATE USER simplified_test with password '[password]';

grant all privileges on database simplified_content_dev to simplified;
grant all privileges on database simplified_content_test to simplified_test;
```

### Data Directory

Create an empty directory to be your data directory:
```sh
$ mkdir YOUR_DATA_DIRECTORY
```

In your content server configuration file, your specified "data_directory" should be YOUR_DATA_DIRECTORY.
