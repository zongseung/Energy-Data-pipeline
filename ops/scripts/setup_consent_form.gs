/**
 * 연구데이터 이용·보안 서약 폼 구성 — 한 번 실행하면 끝납니다.
 * 실행 후 로그(Ctrl+Enter)에 공개 URL이 찍힙니다. 그 주소를 문서에 넣습니다.
 */
function setupForm() {
  var FORM_ID = '1z8ldJVeedL2mHDFzHtw3oIJm9LvnLlMvAxJoO26e-n8';
  var form = FormApp.openById(FORM_ID);

  form.setTitle('연구데이터 이용·보안 서약 제출');
  form.setDescription(
    '전력 연구데이터 접속 권한 신청서입니다. 제출 전 이용조건 · 보안 서약을 읽어주세요.\n' +
    'https://eax-1.gitbook.io/energy-research-data/policy/05-terms\n\n' +
    '이 폼의 제출이 서명을 갈음합니다. 인쇄·날인은 필요 없습니다.'
  );

  // 기존 항목 모두 지우고 새로 만든다 (여러 번 실행해도 결과가 같다)
  var items = form.getItems();
  for (var i = items.length - 1; i >= 0; i--) {
    form.deleteItem(items[i]);
  }

  form.addTextItem()
    .setTitle('성명')
    .setRequired(true);

  form.addTextItem()
    .setTitle('소속 (기관 · 학과 · 연구실)')
    .setRequired(true);

  form.addTextItem()
    .setTitle('이메일')
    .setHelpText(
      '관리자가 접속 명령어를 보내는 주소입니다.\n' +
      '나중에 LibreChat에 가입할 때도 같은 이메일을 쓰니 자주 확인하는 주소를 적어주세요.'
    )
    .setRequired(true);

  form.addTextItem()
    .setTitle('전화번호')
    .setRequired(true);

  var agreements = [
    '이용조건 · 보안 서약 전문을 읽고 동의합니다.',
    '데이터를 본 과제의 연구·분석 목적으로만 사용합니다.',
    '조회 결과를 재배포하지 않고, 논문·보고서에는 요약 통계·그래프로만 포함합니다.',
    '발급받은 자격증명을 본인만 사용하고 타인과 공유하지 않습니다.',
    '질문에 개인정보·비밀번호·공개 금지 내용을 입력하지 않습니다.',
    '모든 쿼리가 감사 로그에 기록됨을 인지합니다.',
    '질문과 조회 결과가 OpenAI API로 전송될 수 있음을 인지합니다.',
    '과제 종료 시 권한 회수와 데이터 파기에 협조합니다.'
  ];

  var check = form.addCheckboxItem();
  check.setTitle('동의 항목 — ' + agreements.length + '개 모두 체크해야 제출됩니다')
       .setChoiceValues(agreements)
       .setRequired(true)
       .setValidation(
         FormApp.createCheckboxValidation()
           .setHelpText('모든 항목에 동의해야 접속 권한을 발급할 수 있습니다.')
           .requireSelectExactly(agreements.length)
           .build()
       );

  form.setCollectEmail(false);          // 이메일은 위 질문으로 직접 받는다
  form.setAllowResponseEdits(false);
  form.setLimitOneResponsePerUser(false);
  form.setConfirmationMessage(
    '제출됐습니다. 관리자 확인 후 적어주신 이메일로 접속 명령어를 보내드립니다.'
  );

  Logger.log('완료. 공개 URL:');
  Logger.log(form.getPublishedUrl());
  Logger.log('편집 URL:');
  Logger.log(form.getEditUrl());
}
