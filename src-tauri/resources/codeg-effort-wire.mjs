// MyClaw hand-declared routes promise canonical gateway wire values, rather
// than merely selectable native labels. Never advertise aliases or budgets.
export function verifiedWireLevels({ runtime, provider, api, map, compat, nativeLevels }) {
  const platform = provider === "myclaw" || provider === "myclaw-anthropic";
  if (!platform) return nativeLevels;
  if (!map || typeof map !== "object" || Array.isArray(map)) return [];
  const nativeProtocol = api === "anthropic-messages"
    ? compat?.forceAdaptiveThinking === true && compat?.supportsMidConvoEffort !== true
    : api === "openai-completions" && compat?.supportsReasoningEffort === true &&
      (compat?.thinkingFormat === "openai" || (runtime === "pi" && compat?.thinkingFormat === undefined));
  if (!nativeProtocol) return [];
  return nativeLevels.filter((level) => map[level] === (level === "off" ? "none" : level));
}
