del /F /S /Q dist
del /F /S /Q build
C:\Users\Administrator\AppData\Local\Programs\Python\Python38\python setup.py sdist bdist_wheel
twine.exe upload dist/*