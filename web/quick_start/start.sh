sudo docker stop stock
sudo docker rm stock
sudo docker build -t stock-app-1.0 .

sudo docker run -it --name stock -p 10023:10023 stock-app-1.0
