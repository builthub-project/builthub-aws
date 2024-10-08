@echo off
SET LAMBDA_NAME=template-step0

echo "Building..."
rd /S /Q .\target

pip install --target .\target\packages requests
cd target\packages
jar -cfM ..\%LAMBDA_NAME%.zip .
cd ..
copy ..\*.py .
jar -ufM %LAMBDA_NAME%.zip lambda_function.py
cd ..
echo "Done..."
