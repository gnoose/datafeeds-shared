# Changelog

Describe changes that require local developer environment updates here.
Include the date and steps required to update.

## Upgrade to python 3.8 - 2021-03-25

Create a new virtual environment and install requirements:

```
pyenv install 3.8.5
pyenv virtualenv 3.8.5 datafeeds38
pyenv activate datafeeds38
pip install --upgrade pip
pip install -r requirements.txt
pip install -r dev-requirements.txt
```

If you get an error like `zipimport.ZipImportError: can't decompress data; zlib not available`:

```
brew reinstall zlib bzip2
export LDFLAGS="-L/usr/local/opt/zlib/lib -L/usr/local/opt/bzip2/lib"
export CPPFLAGS="-I/usr/local/opt/zlib/include -I/usr/local/opt/bzip2/include"
```

then try the steps above again.

(fix from https://github.com/aws/aws-elastic-beanstalk-cli-setup/issues/41)
