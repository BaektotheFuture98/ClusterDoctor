from abc import ABC, abstractmethod


class Notifier(ABC):
    @abstractmethod
    async def notify(
        self,
        message: str,
        *,
        gaps: tuple[str, ...] = (),
        analysis_failed: bool = False,
    ) -> None:
        """리포트를 운영자에게 전달한다.

        ``gaps``와 ``analysis_failed``를 본문에 섞어 넘기지 않고 따로 받는
        이유: 그것을 어떻게 보여줄지는 표현의 관심사다. 이 코드베이스는 그
        경계를 지킨다(로그 줄을 그리는 ``format_log_line``이 도메인이 아니라
        프롬프트 쪽에 있는 것과 같은 이유다). 문자열에 이어 붙이면 모델이 쓴
        본문과 시스템이 덧붙인 것이 구별되지 않는다.

        기본값을 둔 것은 두 값이 없는 호출을 허용하기 위해서다 — 알림 자체는
        진단 결과의 완전성을 모르고도 성립한다.

        Args:
            message:         리포트 본문. 분석이 실패했더라도 항상 전달된다.
            gaps:            수집하지 못한 보조 근거. 리포트는 유효하다.
            analysis_failed: 분석 자체가 실패했는가. 본문을 신뢰할 수 없다는
                             뜻이므로 눈에 띄게 알려야 한다.
        """
        ...
