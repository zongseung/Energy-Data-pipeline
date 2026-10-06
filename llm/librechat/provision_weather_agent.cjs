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
    description: 'NAS 기상예보 조회 및 누락 자료 수집',
    instructions: '한국어로 답한다. 기상예보는 collect-weather-nas Skill을 적용한다. 지역·예보종·요소·기간을 확인한다. 사용자가 수집을 요청한 경우에만 collect_forecast의 confirmed를 true로 전달한다. 조회 SQL은 기존 브라우저 승인 절차를 거친다. 복정동은 경기도 성남시수정구이며 실제 결과와 CSV 링크를 제공한다.',
    // Native create/update methods prune filesystem skill IDs from explicit lists.
    // An empty list with skills enabled exposes the deployment catalog.
    skills_enabled: true, skills: [],
    tools: ['plan_query', 'execute_query', 'collect_forecast', 'forecast_collection_status'].map((name) => `${name}_mcp_energy-db`),
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
