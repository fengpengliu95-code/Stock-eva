import type { SourceView } from "./state";

const SYMBOL_PATTERN = /^(sh|sz)\.[0-9]{6}$/;

export interface SecurityRoute {
  symbol: string;
  sourceView: SourceView;
}

function isSourceView(value: string | null): value is SourceView {
  return value === "portfolio" || value === "watchlists";
}

export function securityHash(symbol: string, sourceView: SourceView): string {
  if (!SYMBOL_PATTERN.test(symbol)) throw new TypeError("invalid security symbol");
  return `#security/${encodeURIComponent(symbol)}?from=${sourceView}`;
}

export function parseSecurityHash(hash: string): SecurityRoute | null {
  const match = /^#security\/([^?]+)(?:\?(.*))?$/.exec(hash);
  if (!match) return null;
  const symbol = decodeURIComponent(match[1]).toLowerCase();
  const sourceView = new URLSearchParams(match[2] ?? "").get("from");
  if (!SYMBOL_PATTERN.test(symbol) || !isSourceView(sourceView)) return null;
  return { symbol, sourceView };
}

export function bindSecurityEntrypoints(
  root: Document,
  navigate: (symbol: string, sourceView: SourceView) => void,
): () => void {
  const onClick = (event: MouseEvent): void => {
    const target = event.target;
    if (!(target instanceof Element)) return;
    const control = target.closest<HTMLElement>("[data-security-symbol]");
    if (!control) return;
    const symbol = control.dataset.securitySymbol?.toLowerCase() ?? "";
    const source = control.dataset.securitySource ?? "";
    if (!SYMBOL_PATTERN.test(symbol) || !isSourceView(source)) return;
    event.preventDefault();
    navigate(symbol, source);
  };
  root.addEventListener("click", onClick);
  return () => root.removeEventListener("click", onClick);
}
