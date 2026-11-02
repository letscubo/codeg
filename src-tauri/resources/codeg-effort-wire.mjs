// MyClaw hand-declared routes promise canonical gateway wire values, rather
// than merely selectable native labels. Never advertise aliases or budgets.
export function samplingPreservesSelection({ modelId, level, wireEffort, samplingParams, samplingParamsByThinkingLevel }) {
  const effective = { ...samplingParams, ...samplingParamsByThinkingLevel?.[level] };
  if (Object.hasOwn(effective, "model") && (typeof modelId !== "string" || effective.model !== modelId)) return false;
  if (Object.hasOwn(effective, "reasoning_effort") &&
      (typeof wireEffort !== "string" || effective.reasoning_effort !== wireEffort)) return false;
  return true;
}

export function verifiedWireLevels({ runtime, provider, api, map, compat, nativeLevels, modelId, samplingParams, samplingParamsByThinkingLevel }) {
  const platform = provider === "myclaw" || provider === "myclaw-anthropic";
  if (!platform) return nativeLevels;
  if (!map || typeof map !== "object" || Array.isArray(map)) return [];
  const nativeProtocol = api === "anthropic-messages"
    ? compat?.forceAdaptiveThinking === true && compat?.supportsMidConvoEffort !== true
    : api === "openai-completions" && compat?.supportsReasoningEffort === true &&
      (compat?.thinkingFormat === "openai" || (runtime === "pi" && compat?.thinkingFormat === undefined));
  if (!nativeProtocol) return [];
  return nativeLevels.filter((level) => {
    const wireEffort = level === "off" ? "none" : level;
    if (map[level] !== wireEffort) return false;
    return runtime !== "pi" || api !== "openai-completions" || samplingPreservesSelection({
      modelId, level, wireEffort, samplingParams, samplingParamsByThinkingLevel,
    });
  });
}
