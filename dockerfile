# 베이스 이미지 설정 (Python 3.9 사용)
FROM python:3.10.5

# 필수 패키지 설치
RUN pip install --upgrade pip
RUN apt-get update && apt-get install -y ca-certificates

# 작업 디렉토리 설정
WORKDIR /app

# # 필요한 디렉토리 생성
# RUN mkdir -p /app/delete_files

# 필요 파일 복사
COPY requirements.txt requirements.txt

# 의존성 설치
RUN pip install -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# # SSL을 무시하도록 설정
# ENV PYTHONHTTPSVERIFY=0

# # 포트 노출
# EXPOSE 5000

# Flask 애플리케이션 실행
CMD ["python", "app.py"]

# 타임존 설정
ENV TZ=Asia/Seoul