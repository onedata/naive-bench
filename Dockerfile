FROM python:3-alpine
  
WORKDIR /root/

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "/root/c-socket.py" ]