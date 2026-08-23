import { flushSync } from "react-dom";

const CARD_SELECTOR = "[data-route-id]";
const LIST_SELECTOR = ".route-column-list";
const WORKSPACE_SELECTOR = ".route-workspace";
const MOTION_DURATION_MS = 650;

interface CardPosition {
  height: number;
  left: number;
  top: number;
  width: number;
}

interface MotionSource {
  ghost: HTMLElement;
  position: CardPosition;
  workspace: HTMLElement;
}

function findCard(routeId: string | null): HTMLElement | null {
  if (routeId === null) return null;
  return [...document.querySelectorAll<HTMLElement>(CARD_SELECTOR)]
    .find((card) => card.dataset.routeId === routeId) ?? null;
}

function scrollDestinationIntoView(
  card: HTMLElement | null,
  behavior: ScrollBehavior,
): CardPosition | null {
  const list = card?.closest<HTMLElement>(LIST_SELECTOR);
  if (!card || !list) return null;

  const cardRect = card.getBoundingClientRect();
  const listRect = list.getBoundingClientRect();
  const centeredScrollTop = list.scrollTop
    + cardRect.top
    - listRect.top
    - ((listRect.height - cardRect.height) / 2);
  const maxScrollTop = Math.max(0, list.scrollHeight - list.clientHeight);
  const targetScrollTop = Math.min(maxScrollTop, Math.max(0, centeredScrollTop));

  if (typeof list.scrollTo === "function") {
    list.scrollTo({ top: targetScrollTop, behavior });
  } else {
    list.scrollTop = targetScrollTop;
  }
  return {
    height: cardRect.height,
    left: cardRect.left,
    top: cardRect.top - (targetScrollTop - list.scrollTop),
    width: cardRect.width,
  };
}

function captureMotionSource(routeId: string | null, reducedMotion: boolean): MotionSource | null {
  const source = findCard(routeId);
  const workspace = source?.closest<HTMLElement>(WORKSPACE_SELECTOR);
  if (reducedMotion || !source?.animate || !workspace) return null;
  const sourceRect = source.getBoundingClientRect();
  return {
    ghost: source.cloneNode(true) as HTMLElement,
    position: {
      height: sourceRect.height,
      left: sourceRect.left,
      top: sourceRect.top,
      width: sourceRect.width,
    },
    workspace,
  };
}

function mountMotionCard(source: MotionSource): HTMLElement {
  const { ghost, position, workspace } = source;
  const workspaceRect = workspace.getBoundingClientRect();

  ghost.classList.add("route-card-motion-ghost", "selected");
  ghost.removeAttribute("data-route-id");
  ghost.removeAttribute("aria-current");
  ghost.setAttribute("aria-hidden", "true");
  ghost.querySelectorAll<HTMLButtonElement>("button").forEach((button) => {
    button.tabIndex = -1;
  });
  Object.assign(ghost.style, {
    height: `${position.height}px`,
    left: `${position.left - workspaceRect.left}px`,
    top: `${position.top - workspaceRect.top}px`,
    width: `${position.width}px`,
  });
  workspace.append(ghost);
  return ghost;
}

/** 작업 상태 갱신과 카드 이동·목록 스크롤을 하나의 전환으로 적용한다. */
export async function updateRoutesWithMotion(
  routeId: string | null,
  update: () => void,
): Promise<void> {
  const reducedMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;
  const source = captureMotionSource(routeId, reducedMotion);

  flushSync(update);

  const destination = findCard(routeId);
  const destinationPosition = scrollDestinationIntoView(
    destination,
    reducedMotion ? "auto" : "smooth",
  );
  if (
    reducedMotion
    || !source
    || !destination
    || !destinationPosition
  ) return;

  const ghost = mountMotionCard(source);
  destination.classList.add("route-card-motion-target");
  const horizontalDistance = destinationPosition.left - source.position.left;
  const verticalDistance = destinationPosition.top - source.position.top;
  const animation = ghost.animate(
    [
      { opacity: 0.82, transform: "translate(0, 0) scale(0.985)" },
      {
        opacity: 1,
        transform: `translate(${horizontalDistance}px, ${verticalDistance}px) scale(1)`,
      },
    ],
    {
      duration: MOTION_DURATION_MS,
      easing: "cubic-bezier(0.22, 1, 0.36, 1)",
    },
  );

  try {
    await animation.finished;
  } catch {
    // 연속 조작으로 애니메이션이 교체돼도 상태 갱신은 이미 적용됐다.
  } finally {
    destination.classList.remove("route-card-motion-target");
    ghost.remove();
  }
}
