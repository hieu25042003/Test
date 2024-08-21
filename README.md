## Script
Các bước cài đặt của từng ứng dụng MinIO và Airflow. Ở đây, MinIO sử dụng Single-Node Single-Drive và Airflow sử dụng version 2.10
## 1. MinIO 

Trước khi chạy, ta phải cài một số thư viện bằng các lệnh sau để cài MinIO Object Storage for Linux
```
### Install MinIO server ###
wget https://dl.min.io/server/minio/release/linux-amd64/archive/minio-20240803043323.0.0-1.x86_64.rpm -O minio.rpm
sudo dnf install minio.rpm
### Launch MinIO server ###
mkdir ~/minio
minio server ~/minio --console-address :9001
### Install the MinIO Client and create a new alias for my local deployment ###
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/mc
mc alias set local http://127.0.0.1:9000 minioadmin minioadmin
mc admin info local
```
Sau khi chạy xong các bước đấy, ta sử dụng file docker compose và chạy lệnh sau
```
docker compose up -d
docker compose logs #Để kiểm tra các thông tin như version,api,webUI...
```
Cuối cùng đăng nhập lên Web qua link local http://127.0.0.1:9000 và username và password đều là minioadmin. Trong WebUI ta có thể tạo keys, users cũng như các budget để chứa các file 

## 2. Airflow

Các bước để cài đặt Airflow trên local
### Cập nhật hệ thống ###
```
sudo apt update
sudo apt upgrade
```
### Cài đặt các gói liên quan ###
```
sudo apt install -y python3-pip python3-dev libmysqlclient-dev
```
### Cài đặt Apache Airflow ###
```
export AIRFLOW_VERSION=2.7.0
export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
export AIRFLOW_PIP_VERSION="$(pip --version | cut -d " " -f 2)"

pip install "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-google" \
    "apache-airflow-providers-http" \
    "apache-airflow-providers-sqlite" \
    "apache-airflow-providers-postgres"
```
### Cài đặt cấu hình Airflow ###
```
mkdir ~/airflow
export AIRFLOW_HOME=~/airflow
airflow db init
```
### Khởi chạy Airflow ###
```
airflow webserver --daemon
airflow scheduler --daemon
```
### Access Airflow ###
```
Truy cập giao diện web Airflow qua địa chỉ local "http://127.0.0.1:8080"
```
## 3. Chạy Pipeline 

Các file python phải để trong thư mục dags của airflow. File download sẽ tự động tạo các file ndjson random và sử dụng multithreading. File pipeline tạo các bước chuyển đổi file và đẩy lên MinIO cùng với đó là sử dụng thread để có thể tối ưu lượng tài nguyên. Cuối cùng file minio_dag sẽ tạo schedule lúc 7h hàng ngày để chạy trên airflow. File Bigquery cũng sẽ tạo bảng nếu chưa tồn tại và sẽ lấy dữ liệu từ MinIO.