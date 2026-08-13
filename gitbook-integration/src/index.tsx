/**
 * 발전소 지도 GitBook Integration
 *
 * GitBook 페이지 안에 <iframe> 을 직접 넣는 것은 CSP 로 막혀 있다. 대신 이
 * integration 이 ContentKit 의 `webframe` 블록을 그려 주고, GitBook 이 매니페스트의
 * contentSecurityPolicy.frame-src 에 등록된 출처만 그 안에 띄운다.
 *
 * 지도 자체는 상태가 없는 단일 HTML 이다(scripts/build_plant_map.py 가 생성).
 * 발전소 마스터 91행을 페이지에 통째로 구워 두므로 여기서 데이터를 넘길 필요가 없다 —
 * API·인증·CORS 가 전부 사라진다. 발전소가 추가되면 생성 스크립트를 다시 돌리고
 * 호스팅에 올리면 끝이고, 이 integration 은 손대지 않는다.
 */
import { createIntegration, createComponent } from '@gitbook/runtime';

/** 지도 HTML 을 올려 둔 주소. 매니페스트의 frame-src 와 반드시 같아야 한다. */
const MAP_URL = 'https://YOUR-HOST.example.com/plant-map.html';

const plantMap = createComponent({
    componentId: 'plant-map',

    async render() {
        return (
            <block>
                <webframe
                    source={{ url: MAP_URL }}
                    aspectRatio={16 / 10}
                />
            </block>
        );
    },
});

export default createIntegration({
    components: [plantMap],
});
