# TND_Migration

### 로컬 Setting1
1. 해당 프로젝트 코드를 관리할 폴더 위치
    - git clone 해당 프로젝트 주소

### 로컬 Setting2
1. 가상환경 생성 (manage.py와 같은 Directory에 생성)
    - python -m venv venv 
2. 가상환경 실행
    - venv\Scripts\activate(source\Scripts\activate)
3. pip 명령어 최신화 반영
    - python -m pip install --upgrade pip
4. 필요한 Module 설치 (requirements.txt에 명시)
    - pip install -r requirements.txt
5. python manage.py makemigrations
6. python manage.py migrate

### 로컬 실행
- python manage.py runserver
