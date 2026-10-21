/* Run inside LibreChat: node < provision_weather_agent.cjs. Idempotent. */
require('module-alias').addAlias('~', '/app/api');
const mongoose = require('mongoose');
const { initializeDeploymentSkills } = require('@librechat/api');
const { Agent, User } = require('/app/api/db/models');
const methods = require('/app/api/models');
const { AccessRoleIds, PrincipalType, Providers, ResourceType } = require('librechat-data-provider');

async function main() {
  const registry = await initializeDeploymentSkills();
  const skill = registry.list().find((item) => item.name === 'collect-weather-nas');
  if (!skill) throw new Error('weather skill is not deployed');
  await mongoose.connect(process.env.MONGO_URI);
  const admin = await User.findOne({ role: 'ADMIN' }).select('_id').lean();
  const author = admin?._id ?? new mongoose.Types.ObjectId('000000000000000000000000');
  const id = 'agent_energy_weather_nas';
  const settings = {
    name: 'NAS 기상 조회', provider: Providers.OPENAI, model: 'gpt-4o-mini',
    description: '에너지·기상 데이터 조회 및 NAS 예보 누락 자료 수집',
    instructions: '한국어로 답한다. 이 서버의 에너지·기상 데이터를 폭넓게 다루며 기상예보 질문은 collect-weather-nas Skill을 참고한다. 일반 질문은 직접 답하고, 데이터가 필요하면 run_sql 로 바로 조회한다(허락받지 않는다). 무슨 데이터인지·컬럼 뜻·기간은 describe_data 로 설명서와 실측 기간을 가져와 답한다. 조건이 조금 모호하면 합리적으로 가정하고 가정을 밝힌다. 사용자가 수집을 요청한 경우에만 collect_forecast의 confirmed를 true로 전달한다. 복정동은 경기도 성남시수정구이며 실제 결과, 사용한 SQL, CSV 링크를 제공한다.',
    // Native create/update methods prune filesystem skill IDs from explicit lists.
    // An empty list with skills enabled exposes the deployment catalog.
    skills_enabled: true, skills: [],
    tools: ['run_sql', 'describe_data', 'collect_forecast', 'forecast_collection_status'].map((name) => `${name}_mcp_energy-db`),
  };
  const existing = await Agent.findOne({ id }).select('_id').lean();
  const agent = existing
    ? await methods.updateAgent({ id }, settings)
    : await methods.createAgent({ id, author, authorName: '운영 관리', ...settings });
  if (!agent.skills_enabled) throw new Error('agent skills were not enabled');
  const viewer = await methods.findRoleByIdentifier(AccessRoleIds.AGENT_VIEWER);
  if (!viewer) throw new Error('agent viewer role is not initialized');
  await methods.grantPermission(PrincipalType.PUBLIC, null, ResourceType.AGENT,
    agent._id, viewer.permBits, author, undefined, viewer._id);
  if (admin) {
    const owner = await methods.findRoleByIdentifier(AccessRoleIds.AGENT_OWNER);
    await methods.grantPermission(PrincipalType.USER, author, ResourceType.AGENT,
      agent._id, owner.permBits, author, undefined, owner._id);
  }
  console.log(JSON.stringify({ agent_id: id, name: settings.name, skill: skill.name,
    skills_enabled: agent.skills_enabled, tools: agent.tools, source: skill.source }));
  await mongoose.disconnect();
}
main().then(() => process.exit(0)).catch((error) => {
  console.error(error.name + ': ' + error.message);
  process.exit(1);
});
