python -m etl --scale 1
python -m benchmark -l benchmark_report\report_x1.txt
rmdir /s /q data\bronze data\silver data\gold data\baseline
python -m etl --scale 2
python -m benchmark -l benchmark_report\report_x2.txt
rmdir /s /q data\bronze data\silver data\gold data\baseline
python -m etl --scale 5
python -m benchmark -l benchmark_report\report_x5.txt
rmdir /s /q data\bronze data\silver data\gold data\baseline
python -m etl --scale 10
python -m benchmark -l benchmark_report\report_x10.txt
rmdir /s /q data\bronze data\silver data\gold data\baseline
python -m etl --scale 20
python -m benchmark -l benchmark_report\report_x20.txt
@REM python -m etl --scale 50
@REM python -m benchmark -l benchmark_report\report_x50.txt
@REM python -m etl --scale 100
@REM python -m benchmark -l benchmark_report\report_x100.txt