@echo off
chcp 65001 >nul
REM ----------------------------------------------------------------------
REM 연구원용 Tailscale 합류 스크립트.
REM 관리자가 보낸 1회용 키와 기기 이름으로 이 PC 를 폐쇄망에 등록한다.
REM - 키는 파일에 저장하지 않는다. 실행할 때 입력받는다.
REM - 비밀값·접속 주소가 없으므로 모든 연구원에게 같은 파일을 줘도 된다.
REM - 사전 조건: Tailscale 설치 (https://tailscale.com/download).
REM   설치 후 트레이의 "Log in" 은 누르지 말 것 (계정 로그인 방식이라 키와 무관).
REM ----------------------------------------------------------------------
setlocal
set "TS=%ProgramFiles%\Tailscale\tailscale.exe"
if not exist "%TS%" (
  echo [오류] Tailscale 이 설치돼 있지 않습니다. https://tailscale.com/download 에서 먼저 설치하세요.
  pause
  exit /b 1
)

set "KEY="
set "NAME="
set /p "KEY=관리자에게 받은 키 (tskey-auth-...): "
set /p "NAME=관리자가 알려준 기기 이름: "
if not defined KEY (
  echo [오류] 키가 비어 있습니다.
  pause
  exit /b 1
)
if not defined NAME (
  echo [오류] 기기 이름이 비어 있습니다.
  pause
  exit /b 1
)

"%TS%" up --auth-key=%KEY% --hostname=%NAME%
if errorlevel 1 (
  echo.
  echo [실패] 키가 만료됐거나 이미 사용됐을 수 있습니다. 관리자에게 새 키를 요청하세요.
  echo        이미 다른 계정으로 로그인한 PC 라면 먼저 "tailscale logout" 을 실행하세요.
  pause
  exit /b 1
)

echo.
echo [완료] 연결됐습니다. 아래 목록에 관리자 호스트가 보이면 정상입니다.
"%TS%" status
echo.
echo 관리자가 안내한 주소로 접속하세요.
pause
