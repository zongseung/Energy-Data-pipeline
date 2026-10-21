/**
 * 서약 폼 게시 — setupForm() 실행 뒤 한 번 더 돌립니다.
 * Drive API 로 만든 폼은 '게시(Publish)' 상태가 꺼져 있어서 비로그인 접속이 401 입니다.
 */
function publishForm() {
  var form = FormApp.openById('1z8ldJVeedL2mHDFzHtw3oIJm9LvnLlMvAxJoO26e-n8');

  try {
    form.setPublished(true);                    // 2025년 추가된 게시 플래그
    Logger.log('setPublished(true) 적용');
  } catch (e) {
    Logger.log('setPublished 미지원: ' + e.message);
  }

  try {
    form.setRequireLogin(false);                // 개인 Gmail 계정은 예외가 날 수 있다
    Logger.log('setRequireLogin(false) 적용');
  } catch (e) {
    Logger.log('setRequireLogin 미지원(개인계정이면 정상): ' + e.message);
  }

  form.setAcceptingResponses(true);

  try { Logger.log('isPublished: ' + form.isPublished()); } catch (e) {}
  Logger.log('응답 받는 중: ' + form.isAcceptingResponses());
  Logger.log('질문 수: ' + form.getItems().length);
  Logger.log('공개 URL: ' + form.getPublishedUrl());
}
