-- P9-13: 여수 연료 확정 + 분당 용량 신뢰도 정리 (2026-10-21)
--
-- ① 여수 2기 → coal.  P9-10 에서 근거가 없어 thermal 로 남겨뒀던 것을 확정한다.
--    1977년 **중유**로 준공했으나 순환유동층 보일러로 교체해 **유연탄(공식 표기 아역청탄)**
--    으로 전환했다. 남동발전 공식 발전현황의 용량(1호기 340MW, 2호기 328.6MW)이
--    우리 capacity_mw 와 일치하므로 같은 설비다. 바이오매스 10% 혼소라 혼소율이 더 오르면
--    biomass 재검토 대상이다.
--    → thermal 값은 이제 비어 있다. 미분류 전원이 생기면 다시 쓴다.
--
-- ② 분당 CG 8기의 capacity_mw 를 NULL 로 비운다.
--    상관 분석으로 블록 매핑은 맞다고 확인했다 (ST1 상관 CG1 0.94·CG3 0.84·CG5 0.80 /
--    ST2 상관 CG6 0.82·CG7 0.81·CG8 0.86 — capacities.py 의 "CG1~5=1블록, CG6~8=2블록" 과 일치).
--    그런데 **실측 최대 시간발전이 정격을 크게 넘는다**: CG5 1,164MW · CG1 645MW · CG3 567MW
--    vs 정격 77.75MW. GT 한 기가 그만큼 낼 수 없다.
--    원천이 호기별이 아니라 블록 합계를 특정 호기에 몰아 보내는 것으로 보이나 확정 못 했다.
--    **틀린 값을 두면 이용률·초과 검사가 전부 오염된다** → 비우고 note 로 남긴다.
--    (ST1 245MW/정격 185MW, ST2 352MW/정격 115MW 도 초과하므로 ST 도 함께 비운다)
BEGIN;

UPDATE plants SET fuel_type = 'coal'
 WHERE operator='namdong' AND fuel_type='thermal' AND site_name='여수발전본부';

UPDATE plants SET capacity_mw = NULL
 WHERE operator='namdong' AND site_name='분당발전본부' AND fuel_type='combined';

COMMIT;
