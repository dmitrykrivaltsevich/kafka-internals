export const meta = {
  name: 'kafka-arch-critic',
  description: 'Cross-chapter consistency + completeness critique and accuracy check of the synthesized overview/glossary',
  phases: [{ title: 'Critique', detail: 'multiple lenses read the finished site and report findings' }]
};

const ROOT = '/Users/user/projects/_playground/apache-kafka-architecture/kafka-source';
const OUT  = '/Users/user/projects/_playground/apache-kafka-architecture';
const F = OUT + '/_fragments/';

const ALL = ['00-overview','01-record-format','02-wire-protocol','03-storage-log-engine','04-storage-management',
  '05-tiered-storage','06-network-and-threading','07-request-processing','08-replication','09-fetch-path',
  '10-kraft-consensus','11-kraft-controller','12-metadata-propagation','13-group-coordination','14-transactions-eos',
  '15-share-groups','16-producer-client','17-consumer-client','18-security','19-quotas','20-kafka-streams',
  '21-kafka-connect','glossary'];
const paths = (arr) => arr.map((s) => F + s + '.html').join('\n  ');

const FINDINGS_SCHEMA = {
  type: 'object',
  properties: {
    lens: { type: 'string' },
    filesRead: { type: 'array', items: { type: 'string' } },
    findings: { type: 'array', items: { type: 'object', properties: {
      severity: { type: 'string', enum: ['critical', 'major', 'minor'] },
      location: { type: 'string', description: 'chapter(s) + section/quote' },
      issue: { type: 'string' },
      suggestedFix: { type: 'string', description: 'grounded in source where factual; cite path:line' }
    }, required: ['severity', 'location', 'issue', 'suggestedFix'] } },
    overallNote: { type: 'string' }
  },
  required: ['lens', 'findings']
};

async function withRetry(items, fn, rounds) {
  rounds = rounds || 2;
  let out = await parallel(items.map((it) => () => fn(it)));
  for (let r = 1; r <= rounds; r++) {
    const todo = items.filter((it, i) => out[i] == null);
    if (!todo.length) break;
    log('Retry ' + r + ' for ' + todo.length);
    const redo = await parallel(todo.map((it) => () => fn(it)));
    todo.forEach((it, k) => { out[items.indexOf(it)] = redo[k]; });
  }
  return out;
}

const KRAFT_NOTE = 'Remember this is Apache Kafka 4.4.0-SNAPSHOT, KRaft-only (ZooKeeper removed); flag any statement that treats ZooKeeper as current. Ground truth is source under ' + ROOT + '.';

const LENSES = [
  { k: 'overview-accuracy',
    p: 'Adversarially fact-check the TOP-LEVEL OVERVIEW. Read:\n  ' + F + '00-overview.html\nand cross-check its claims against ' + OUT + '/_research/chapter-briefs.json (the verified per-chapter keyFacts) and, where a claim is concrete, against the actual source. Flag: factual errors, overclaims, anything inconsistent with the detailed chapters, wrong defaults, broken end-to-end narrative, and any broken cross-links (href to a file that is not in the chapter set). ' + KRAFT_NOTE },
  { k: 'glossary-accuracy',
    p: 'Adversarially fact-check the GLOSSARY. Read:\n  ' + F + 'glossary.html\nand verify each definition for correctness and consistency with the chapters (use ' + OUT + '/_research/chapter-briefs.json). Flag wrong/imprecise definitions, terms whose definition contradicts the chapter that owns them, and missing high-value terms. ' + KRAFT_NOTE },
  { k: 'consistency-foundations',
    p: 'Check CROSS-CHAPTER CONSISTENCY across the foundational/storage/replication chapters. Read these and compare:\n  ' + paths(ALL.slice(0, 12)) + '\nLook for the SAME fact stated differently in two chapters (e.g. a config default, a byte offset, an algorithm step, an epoch rule), contradictions, and terminology used inconsistently. Report each conflict with both locations and which one the source supports (cite path:line). ' + KRAFT_NOTE },
  { k: 'consistency-coordination-clients',
    p: 'Check CROSS-CHAPTER CONSISTENCY across the KRaft/coordination/clients/cross-cutting chapters. Read these and compare:\n  ' + paths(ALL.slice(12)) + '\n  ' + F + '00-overview.html\nLook for contradictions, the same fact stated differently, and inconsistent terminology (e.g. epochs, coordinators, rebalance protocol details, EOS). Report each with both locations and which the source supports (cite path:line). ' + KRAFT_NOTE },
  { k: 'completeness',
    p: 'Assess COMPLETENESS of the whole guide. Read ' + OUT + '/_research/chapter-briefs.json (abstracts + section lists + keyFacts of all 21 detailed chapters) and ' + F + '00-overview.html. Identify any MAJOR Kafka subsystem, mechanism, or concept that is missing or materially under-covered across the guide, and any place the overview promises something the chapters do not deliver. Prioritize: only flag genuinely important gaps, not nitpicks. ' + KRAFT_NOTE }
];

phase('Critique');
const reports = await withRetry(LENSES, (l) => agent(l.p + '\n\nReturn structured findings; for factual ones, ground the suggested fix in source with a path:line. Default to reporting when unsure, but keep severity honest (critical = wrong/misleading core claim; major = wrong detail; minor = polish).',
  { label: 'critic:' + l.k, phase: 'Critique', schema: FINDINGS_SCHEMA, agentType: 'general-purpose' }));

const all = reports.filter(Boolean).flatMap((r) => (r.findings || []).map((f) => ({ lens: r.lens, ...f })));
const crit = all.filter((f) => f.severity === 'critical').length;
const maj = all.filter((f) => f.severity === 'major').length;
log('Critique complete: ' + all.length + ' findings (' + crit + ' critical, ' + maj + ' major).');
return { reports, summary: { total: all.length, critical: crit, major: maj } };
