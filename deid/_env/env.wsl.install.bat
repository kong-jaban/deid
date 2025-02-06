@rem distro 확인
@rem wsl --list --online

@echo linux 사용자는 반드시 ups 로 만드시오
@echo 윈도 vscode에 wsl extension 설치하시오
pause

set distro_name=Ubuntu-22.04

wsl --install --distribution %distro_name%
wsl --set-default %distro_name%
