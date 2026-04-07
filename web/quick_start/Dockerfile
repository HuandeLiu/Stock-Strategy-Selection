
FROM stock-base
# 创建工作目录
WORKDIR /vms/sdb/lhd/code/paper/stock/analysis/demo/core
RUN mkdir -p /vms/sdb/lhd/code/paper/stock/analysis/demo/core/data

# 复制 Java 和 Python 文件到容器中
COPY web-0.1.2-stock.jar .
COPY ts_code.csv .
COPY returnTodb.py .
COPY dataupdate.py .
COPY data_analysis.py .
COPY write_data.py .
COPY conf.ini .

RUN (crontab -l 2>/dev/null; echo "1 0 * * * python /vms/sdb/lhd/code/paper/stock/analysis/demo/core/write_data.py >> /vms/sdb/lhd/code/paper/stock/analysis/demo/core/write_data.log 2>&1") | crontab -
#RUN (crontab -l 2>/dev/null; echo "10 22 * * * python /vms/sdb/lhd/code/paper/stock/analysis/demo/core/write_data.py >> /vms/sdb/lhd/code/paper/stock/analysis/demo/core/write_data.log 2>&1") | crontab -
#RUN (crontab -l 2>/dev/null; echo "10 17 * * * python /vms/sdb/lhd/code/paper/stock/analysis/demo/core/dataupdate.py >> /vms/sdb/lhd/code/paper/stock/analysis/demo/core/dataAnalysis.log 2>&1") | crontab -
# 设置容器启动时运行的命令
#CMD ["python" ,"write_data.py"]
#CMD ["java", "-jar", "web-0.1.2-stock.jar"]
CMD ["sh","-c","cron && python write_data.py && java -jar web-0.1.2-stock.jar"]
