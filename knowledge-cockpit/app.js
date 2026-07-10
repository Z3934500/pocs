(function () {
  const data = window.KNOWLEDGE_COCKPIT_DATA;
  const publicViews = new Set(["map", "explore", "script", "qa", "graph"]);
  const state = {
    view: "map",
    lastPublicView: "map",
    query: "",
    category: "All",
    selectedTermId: data.terms[0].id,
    selectedStepId: data.demoFlow[0].id,
    selectedStoryId: data.storyMap.groups[0].items[0].id
  };

  const API_BASE = window.location.pathname.startsWith("/knowledge-cockpit") ? "/knowledge-cockpit/api" : "/api";

  const remote = {
    role: "screen",
    session: "demo",
    enabled: false,
    revision: -1,
    pushTimer: null,
    installPrompt: null,
    pinRequired: false,
    presenterUnlocked: false,
    pin: sessionStorage.getItem("kcPresenterPin") || ""
  };

  function apiUrl(path) {
    return API_BASE + path;
  }

  const els = {
    viewTitle: document.getElementById("viewTitle"),
    searchInput: document.getElementById("searchInput"),
    sessionInput: document.getElementById("sessionInput"),
    presenterPinInput: document.getElementById("presenterPinInput"),
    unlockButton: document.getElementById("unlockButton"),
    syncStatus: document.getElementById("syncStatus"),
    categoryPanel: document.getElementById("categoryPanel"),
    storyGroups: document.getElementById("storyGroups"),
    storyDetail: document.getElementById("storyDetail"),
    storyDeckLabel: document.getElementById("storyDeckLabel"),
    storyHeadline: document.getElementById("storyHeadline"),
    storyIntro: document.getElementById("storyIntro"),
    termGrid: document.getElementById("termGrid"),
    scriptList: document.getElementById("scriptList"),
    scriptDetail: document.getElementById("scriptDetail"),
    qaList: document.getElementById("qaList"),
    graphMap: document.getElementById("graphMap"),
    aiQuestion: document.getElementById("aiQuestion"),
    aiAnswer: document.getElementById("aiAnswer"),
    aiStatus: document.getElementById("aiStatus"),
    askAiButton: document.getElementById("askAiButton"),
    voiceButton: document.getElementById("voiceButton"),
    useCurrentContextButton: document.getElementById("useCurrentContextButton"),
    selectedTitle: document.getElementById("selectedTitle"),
    selectedDetail: document.getElementById("selectedDetail"),
    installButton: document.getElementById("installButton"),
    copyNotesButton: document.getElementById("copyNotesButton"),
  };

  function init() {
    const params = new URLSearchParams(window.location.search);
    remote.role = params.get("role") === "remote" || params.get("role") === "presenter" ? "remote" : "screen";
    remote.session = normalizeSession(params.get("session") || remote.session);
    els.sessionInput.value = remote.session;
    els.presenterPinInput.value = remote.pin;

    document.querySelectorAll(".nav-tab").forEach((button) => {
      button.addEventListener("click", () => setView(button.dataset.view, { push: true }));
    });

    document.querySelectorAll(".role-button").forEach((button) => {
      button.addEventListener("click", () => setRemoteRole(button.dataset.role));
    });

    els.searchInput.addEventListener("input", (event) => {
      state.query = event.target.value.trim().toLowerCase();
      renderExplore();
      schedulePush();
    });

    els.sessionInput.addEventListener("change", () => {
      remote.session = normalizeSession(els.sessionInput.value);
      els.sessionInput.value = remote.session;
      remote.revision = -1;
      setSyncStatus("Session: " + remote.session);
      if (remote.role === "remote") {
        pushRemoteState();
      }
    });

    els.presenterPinInput.addEventListener("change", () => {
      remote.pin = els.presenterPinInput.value.trim();
      remote.presenterUnlocked = false;
      if (remote.pin) {
        sessionStorage.setItem("kcPresenterPin", remote.pin);
      } else {
        sessionStorage.removeItem("kcPresenterPin");
      }
      renderPresenter();
    });

    els.unlockButton.addEventListener("click", () => unlockPresenter(false));
    els.copyNotesButton.addEventListener("click", copyPresenterNotes);
    els.installButton.addEventListener("click", installPwa);
    els.askAiButton.addEventListener("click", askAi);
    setupVoiceInput();
    els.useCurrentContextButton.addEventListener("click", useCurrentContextForAi);

    window.addEventListener("beforeinstallprompt", (event) => {
      event.preventDefault();
      remote.installPrompt = event;
      els.installButton.disabled = false;
      els.installButton.textContent = "Install";
    });

    els.installButton.disabled = true;
    els.installButton.textContent = "Install";

    applyRoleUi();
    registerServiceWorker();
    renderAll();
    checkServer().then(() => {
      if (remote.role === "remote" && remote.pin) {
        unlockPresenter(true);
      } else {
        renderPresenter();
      }
    });
    setInterval(pollRemoteState, 900);
  }

  function renderAll() {
    renderViewState();
    renderStoryMap();
    renderCategories();
    renderExplore();
    renderScript();
    renderQa();
    renderGraph();
    renderPresenter();
  }

  function isPublicView(view) {
    return publicViews.has(view);
  }

  function setView(view, options = {}) {
    if (view === "ai" && remote.role !== "remote") {
      setRemoteRole("remote");
    }
    state.view = view;
    if (isPublicView(view)) {
      state.lastPublicView = view;
    }
    renderViewState();
    if (options.push && isPublicView(view)) {
      schedulePush();
    }
  }

  function renderViewState() {
    document.querySelectorAll(".nav-tab").forEach((button) => {
      button.classList.toggle("active", button.dataset.view === state.view);
    });
    document.querySelectorAll(".view").forEach((section) => {
      section.classList.toggle("active", section.id === state.view + "View");
    });

    const titles = {
      map: "Story Map",
      explore: "Explore",
      script: "Demo Script",
      qa: "Q&A",
      graph: "Concept Graph",
      ai: "Private AI Notes"
    };
    els.viewTitle.textContent = titles[state.view] || "Explore";
  }

  function setRemoteRole(role) {
    remote.role = role;
    if (role === "screen" && state.view === "ai") {
      state.view = state.lastPublicView;
    }
    applyRoleUi();
    if (role === "remote") {
      if (!remote.pinRequired) {
        remote.presenterUnlocked = true;
      } else if (remote.pin) {
        unlockPresenter(true);
      }
      pushRemoteState();
    }
    renderAll();
  }

  function applyRoleUi() {
    document.body.classList.toggle("role-screen", remote.role === "screen");
    document.body.classList.toggle("role-remote", remote.role === "remote");
    document.querySelectorAll(".role-button").forEach((button) => {
      button.classList.toggle("active", button.dataset.role === remote.role);
    });
    setSyncStatus(remote.enabled ? roleLabel() : "Static mode");
  }

  function renderCategories() {
    const counts = data.terms.reduce(
      (acc, term) => {
        acc[term.category] = (acc[term.category] || 0) + 1;
        acc.All += 1;
        return acc;
      },
      { All: 0 }
    );
    const categories = Object.keys(counts).sort((a, b) => (a === "All" ? -1 : b === "All" ? 1 : a.localeCompare(b)));

    els.categoryPanel.innerHTML = `<span class="category-title">Categories</span>${categories
      .map(
        (category) => `
          <button class="category-button ${category === state.category ? "active" : ""}" data-category="${escapeAttr(category)}">
            <span>${escapeHtml(category)}</span>
            <span>${counts[category]}</span>
          </button>`
      )
      .join("")}`;

    els.categoryPanel.querySelectorAll(".category-button").forEach((button) => {
      button.addEventListener("click", () => {
        state.category = button.dataset.category;
        renderCategories();
        renderExplore();
        schedulePush();
      });
    });
  }

  function renderStoryMap() {
    if (!data.storyMap || !els.storyGroups || !els.storyDetail) {
      return;
    }

    els.storyDeckLabel.textContent = data.storyMap.headline;
    els.storyHeadline.textContent = "Project narrative map";
    els.storyIntro.textContent = data.storyMap.intro;

    els.storyGroups.innerHTML = data.storyMap.groups
      .map((group) => {
        const groupClass = `story-group story-group-${escapeAttr(group.id)}`;
        const items = group.items
          .map((item) => {
            const selected = item.id === state.selectedStoryId ? "selected" : "";
            return `
              <button class="story-card ${selected}" data-story-id="${escapeAttr(item.id)}" type="button">
                <span class="story-eyebrow">${escapeHtml(item.eyebrow)}</span>
                <strong>${escapeHtml(item.title)}</strong>
                <span>${escapeHtml(item.oneLiner)}</span>
              </button>`;
          })
          .join("");

        return `
          <details class="${groupClass}" ${group.defaultOpen ? "open" : ""}>
            <summary>
              <span class="story-number">${escapeHtml(group.number)}</span>
              <span>
                <strong>${escapeHtml(group.label)}</strong>
                <em>${escapeHtml(group.title)}</em>
              </span>
            </summary>
            <p class="muted">${escapeHtml(group.summary)}</p>
            <div class="story-items">${items}</div>
          </details>`;
      })
      .join("");

    els.storyGroups.querySelectorAll(".story-card").forEach((button) => {
      button.addEventListener("click", () => {
        state.selectedStoryId = button.dataset.storyId;
        const story = getSelectedStory();
        if (story?.item?.anchorTermId) {
          state.selectedTermId = story.item.anchorTermId;
        }
        renderStoryMap();
        renderPresenter();
        schedulePush();
      });
    });

    const story = getSelectedStory();
    if (!story) {
      els.storyDetail.innerHTML = `<p class="muted">No story selected.</p>`;
      return;
    }

    const { item, group } = story;
    els.storyDetail.innerHTML = `
      <span class="pill accent">${escapeHtml(group.label)}</span>
      <h3>${escapeHtml(item.title)}</h3>
      <p>${escapeHtml(item.oneLiner)}</p>
      <div class="detail-section">
        <h4>Why This Exists</h4>
        <p>${escapeHtml(item.talkTrack)}</p>
      </div>
      <div class="detail-section">
        <h4>Evidence</h4>
        <ul class="notes-list">
          ${item.evidence.map((point) => `<li>${escapeHtml(point)}</li>`).join("")}
        </ul>
      </div>
      <div class="repo-links">
        <a href="../${escapeAttr(item.repoRef)}" target="_blank" rel="noreferrer">Open source: ${escapeHtml(item.repoRef)}</a>
      </div>`;
  }

  function getStoryItems() {
    if (!data.storyMap) {
      return [];
    }
    return data.storyMap.groups.flatMap((group) => group.items.map((item) => ({ group, item })));
  }

  function getSelectedStory() {
    const stories = getStoryItems();
    return stories.find((story) => story.item.id === state.selectedStoryId) || stories[0];
  }
  function renderExplore() {
    const terms = filterTerms();
    els.termGrid.innerHTML = terms.length
      ? terms.map(renderTermCard).join("")
      : `<div class="empty-state">No matching terms.</div>`;

    els.termGrid.querySelectorAll(".term-card").forEach((card) => {
      card.addEventListener("click", () => {
        state.selectedTermId = card.dataset.termId;
        renderExplore();
        renderPresenter();
        schedulePush();
      });
    });
  }

  function filterTerms() {
    return data.terms.filter((term) => {
      const categoryMatch = state.category === "All" || term.category === state.category;
      const haystack = [term.title, term.category, term.oneLiner, term.explain, ...term.tags, ...term.related]
        .join(" ")
        .toLowerCase();
      const queryMatch = !state.query || haystack.includes(state.query);
      return categoryMatch && queryMatch;
    });
  }

  function renderTermCard(term) {
    const selected = term.id === state.selectedTermId ? "selected" : "";
    return `
      <article class="term-card ${selected}" data-term-id="${escapeAttr(term.id)}">
        <div>
          <div class="term-meta">
            <span class="pill accent">${escapeHtml(term.category)}</span>
            ${term.tags.map((tag) => `<span class="pill">${escapeHtml(tag)}</span>`).join("")}
          </div>
        </div>
        <h3>${escapeHtml(term.title)}</h3>
        <p>${escapeHtml(term.oneLiner)}</p>
        <p class="muted">${escapeHtml(term.talkTrack)}</p>
      </article>`;
  }

  function renderScript() {
    els.scriptList.innerHTML = data.demoFlow
      .map(
        (step, index) => `
          <article class="script-step ${step.id === state.selectedStepId ? "active" : ""}" data-step-id="${escapeAttr(step.id)}">
            <span class="pill">${index + 1}. ${escapeHtml(step.duration)}</span>
            <h3>${escapeHtml(step.title)}</h3>
            <p class="muted">${escapeHtml(step.goal)}</p>
          </article>`
      )
      .join("");

    els.scriptList.querySelectorAll(".script-step").forEach((step) => {
      step.addEventListener("click", () => {
        state.selectedStepId = step.dataset.stepId;
        renderScript();
        schedulePush();
      });
    });

    const selected = data.demoFlow.find((step) => step.id === state.selectedStepId) || data.demoFlow[0];
    els.scriptDetail.innerHTML = `
      <span class="pill accent">${escapeHtml(selected.duration)}</span>
      <h3>${escapeHtml(selected.title)}</h3>
      <p>${escapeHtml(selected.goal)}</p>
      <div class="repo-links">
        <a href="../${escapeAttr(selected.open)}" target="_blank" rel="noreferrer">Open source: ${escapeHtml(selected.open)}</a>
      </div>
      <ul class="notes-list">
        ${selected.talkingPoints.map((point) => `<li>${escapeHtml(point)}</li>`).join("")}
      </ul>
      ${selected.image ? `<img src="${escapeAttr(selected.image)}" alt="${escapeAttr(selected.title)}" />` : ""}
    `;
  }

  function renderQa() {
    els.qaList.innerHTML = data.questions
      .map(
        (item) => `
          <article class="qa-card">
            <span class="pill accent">Prepared answer</span>
            <h3>${escapeHtml(item.question)}</h3>
            <p><strong>Short:</strong> ${escapeHtml(item.shortAnswer)}</p>
            <p class="muted">${escapeHtml(item.answer)}</p>
            <button class="icon-button private-action" data-question-id="${escapeAttr(item.id)}">Use Private Notes</button>
          </article>`
      )
      .join("");

    els.qaList.querySelectorAll("button").forEach((button) => {
      button.addEventListener("click", () => {
        const item = data.questions.find((question) => question.id === button.dataset.questionId);
        if (item) {
          setRemoteRole("remote");
          els.selectedTitle.textContent = item.question;
          els.selectedDetail.innerHTML = `
            <div class="detail-section">
              <h4>Short Answer</h4>
              <p>${escapeHtml(item.shortAnswer)}</p>
            </div>
            <div class="detail-section">
              <h4>Expanded Answer</h4>
              <p>${escapeHtml(item.answer)}</p>
            </div>`;
        }
      });
    });
  }

  function renderGraph() {
    els.graphMap.innerHTML = data.graph
      .map(
        ([from, to]) => `
          <div class="edge">
            <span class="node">${escapeHtml(from)}</span>
            <span class="arrow">-></span>
            <span class="node">${escapeHtml(to)}</span>
          </div>`
      )
      .join("");
  }

  function renderPresenter() {
    if (remote.role !== "remote") {
      return;
    }
    if (!hasPresenterAccess()) {
      renderLockedPresenter();
      return;
    }

    if (state.view === "map") {
      renderStoryPresenter();
      return;
    }

    const term = data.terms.find((item) => item.id === state.selectedTermId) || data.terms[0];
    els.selectedTitle.textContent = term.title;
    els.selectedDetail.innerHTML = `
      <div class="detail-section">
        <h4>One-Liner</h4>
        <p>${escapeHtml(term.oneLiner)}</p>
      </div>
      <div class="detail-section">
        <h4>Talk Track</h4>
        <p>${escapeHtml(term.talkTrack)}</p>
      </div>
      <div class="detail-section">
        <h4>Explanation</h4>
        <p>${escapeHtml(term.explain)}</p>
      </div>
      <div class="detail-section">
        <h4>Related</h4>
        <div class="term-meta">
          ${term.related.map((related) => `<span class="pill">${escapeHtml(findTitle(related))}</span>`).join("")}
        </div>
      </div>
      <div class="detail-section">
        <h4>Repo Evidence</h4>
        <div class="repo-links">
          ${term.repoRefs
            .map((ref) => `<a href="../${escapeAttr(ref)}" target="_blank" rel="noreferrer">${escapeHtml(ref)}</a>`)
            .join("")}
        </div>
      </div>`;
  }
  function renderStoryPresenter() {
    const story = getSelectedStory();
    if (!story) {
      return;
    }
    const { item, group } = story;
    els.selectedTitle.textContent = item.title;
    els.selectedDetail.innerHTML = `
      <div class="detail-section">
        <h4>Story Layer</h4>
        <p>${escapeHtml(group.label)} - ${escapeHtml(group.title)}</p>
      </div>
      <div class="detail-section">
        <h4>One-Liner</h4>
        <p>${escapeHtml(item.oneLiner)}</p>
      </div>
      <div class="detail-section">
        <h4>Talk Track</h4>
        <p>${escapeHtml(item.talkTrack)}</p>
      </div>
      <div class="detail-section">
        <h4>Evidence</h4>
        <ul class="notes-list">
          ${item.evidence.map((point) => `<li>${escapeHtml(point)}</li>`).join("")}
        </ul>
      </div>
      <div class="detail-section">
        <h4>Repo Evidence</h4>
        <div class="repo-links">
          <a href="../${escapeAttr(item.repoRef)}" target="_blank" rel="noreferrer">${escapeHtml(item.repoRef)}</a>
        </div>
      </div>`;
  }
  function renderLockedPresenter() {
    els.selectedTitle.textContent = "Private Notes Locked";
    els.selectedDetail.innerHTML = `
      <div class="detail-section">
        <h4>Presenter PIN</h4>
        <p>Enter the presenter PIN on your phone to show private notes and use AI KB.</p>
      </div>`;
  }

  function hasPresenterAccess() {
    return remote.role === "remote" && (!remote.pinRequired || remote.presenterUnlocked);
  }

  function findTitle(id) {
    const term = data.terms.find((item) => item.id === id);
    return term ? term.title : id;
  }

  function useCurrentContextForAi() {
    if (!hasPresenterAccess()) {
      renderLockedPresenter();
      setSyncStatus("Unlock presenter notes first");
      return;
    }
    els.aiQuestion.value = getCurrentNotes();
    setView("ai", { push: false });
  }

  async function askAi() {
    if (!hasPresenterAccess()) {
      els.aiStatus.textContent = "Unlock presenter notes first.";
      renderLockedPresenter();
      return;
    }

    const question = els.aiQuestion.value.trim();
    if (!question) {
      els.aiStatus.textContent = "Type a question first.";
      return;
    }

    els.aiStatus.textContent = "Asking private repo knowledge base...";
    els.askAiButton.disabled = true;
    els.aiAnswer.innerHTML = `<p class="muted">Thinking...</p>`;

    try {
      const response = await fetch(apiUrl("/chat"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, top_k: 5, pin: remote.pin })
      });
      const payload = await response.json();
      if (!response.ok || payload.error) {
        throw new Error(payload.error || "AI request failed.");
      }
      els.aiStatus.textContent = "Private answer generated from repo context.";
      els.aiAnswer.innerHTML = `
        <div class="detail-section">
          <h4>Answer</h4>
          <p>${formatAnswer(payload.answer)}</p>
        </div>
        <div class="detail-section">
          <h4>Sources</h4>
          <div class="repo-links">
            ${(payload.sources || [])
              .map((source) => `<a href="../${escapeAttr(source.path)}" target="_blank" rel="noreferrer">${escapeHtml(source.path)} - ${escapeHtml(source.heading)}</a>`)
              .join("")}
          </div>
        </div>`;
    } catch (error) {
      els.aiStatus.textContent = "AI request failed.";
      els.aiAnswer.innerHTML = `<p class="muted">${escapeHtml(error.message)}</p>`;
    } finally {
      els.askAiButton.disabled = false;
    }
  }

  function setupVoiceInput() {
    if (!els.voiceButton) {
      return;
    }

    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    const speech = {
      recognition: null,
      listening: false,
      baseText: "",
      finalText: ""
    };

    const start = (event) => {
      event.preventDefault();
      if (!hasPresenterAccess()) {
        els.aiStatus.textContent = "Unlock presenter notes first.";
        renderLockedPresenter();
        return;
      }
      if (!SpeechRecognition) {
        els.aiStatus.textContent = "Speech input is not supported by this browser.";
        return;
      }
      if (speech.listening) {
        return;
      }

      speech.recognition = new SpeechRecognition();
      speech.recognition.lang = (navigator.language || "en-US").toLowerCase().startsWith("zh") ? "zh-CN" : "en-US";
      speech.recognition.interimResults = true;
      speech.recognition.continuous = true;
      speech.baseText = els.aiQuestion.value.trim();
      speech.finalText = "";
      speech.listening = true;
      els.voiceButton.classList.add("listening");
      els.voiceButton.textContent = "Listening";
      els.aiStatus.textContent = "Listening while you hold the button...";

      speech.recognition.onresult = (resultEvent) => {
        let interimText = "";
        for (let index = resultEvent.resultIndex; index < resultEvent.results.length; index += 1) {
          const transcript = resultEvent.results[index][0].transcript.trim();
          if (resultEvent.results[index].isFinal) {
            speech.finalText = `${speech.finalText} ${transcript}`.trim();
          } else {
            interimText = `${interimText} ${transcript}`.trim();
          }
        }
        els.aiQuestion.value = [speech.baseText, speech.finalText, interimText].filter(Boolean).join("\n");
      };

      speech.recognition.onerror = (errorEvent) => {
        els.aiStatus.textContent = errorEvent.error ? `Speech input: ${errorEvent.error}` : "Speech input stopped.";
      };

      speech.recognition.onend = () => {
        speech.listening = false;
        els.voiceButton.classList.remove("listening");
        els.voiceButton.textContent = "Voice";
        if (els.aiQuestion.value.trim()) {
          els.aiStatus.textContent = "Voice input captured. Tap Ask AI when ready.";
        }
      };

      speech.recognition.start();
    };

    const stop = () => {
      if (speech.recognition && speech.listening) {
        speech.recognition.stop();
      }
    };

    els.voiceButton.addEventListener("pointerdown", start);
    els.voiceButton.addEventListener("pointerup", stop);
    els.voiceButton.addEventListener("pointercancel", stop);
    els.voiceButton.addEventListener("pointerleave", stop);
    els.voiceButton.addEventListener("contextmenu", (event) => event.preventDefault());
  }
  function formatAnswer(answer) {
    return escapeHtml(answer).replace(/\n/g, "<br>");
  }

  function getCurrentNotes() {
    if (state.view === "map") {
      const story = getSelectedStory();
      if (story) {
        const { item, group } = story;
        return `${group.label}: ${item.title}\n\n${item.oneLiner}\n\n${item.talkTrack}\n\nEvidence:\n- ${item.evidence.join("\n- ")}\n\nSource: ${item.repoRef}`;
      }
    }
    if (state.view === "script") {
      const step = data.demoFlow.find((item) => item.id === state.selectedStepId) || data.demoFlow[0];
      return `${step.title}\n\n${step.goal}\n\n${step.talkingPoints.map((point) => `- ${point}`).join("\n")}`;
    }
    const term = data.terms.find((item) => item.id === state.selectedTermId) || data.terms[0];
    return `${term.title}\n\n${term.oneLiner}\n\n${term.talkTrack}\n\n${term.explain}`;
  }
  function copyPresenterNotes() {
    if (!hasPresenterAccess()) {
      renderLockedPresenter();
      return;
    }
    const notes = getCurrentNotes();
    const markCopied = () => {
      els.copyNotesButton.textContent = "Copied";
      setTimeout(() => {
        els.copyNotesButton.textContent = "Copy";
      }, 1400);
    };

    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(notes).then(markCopied).catch(() => {
        window.prompt("Copy presenter notes:", notes);
      });
      return;
    }

    window.prompt("Copy presenter notes:", notes);
  }

  async function checkServer() {
    try {
      const response = await fetch(apiUrl("/health"), { cache: "no-store" });
      if (!response.ok) {
        throw new Error("server unavailable");
      }
      const payload = await response.json();
      remote.enabled = true;
      remote.pinRequired = Boolean(payload.presenter_pin_required);
      remote.presenterUnlocked = !remote.pinRequired;
      setSyncStatus(`${roleLabel()} | chunks ${payload.chunks} | AI ${payload.openai_configured ? "ready" : "needs key"} | PIN ${remote.pinRequired ? "on" : "off"}`);
      renderPresenter();
    } catch (_) {
      remote.enabled = false;
      remote.pinRequired = false;
      remote.presenterUnlocked = true;
      setSyncStatus("Static mode");
      renderPresenter();
    }
  }

  async function unlockPresenter(silent) {
    remote.pin = els.presenterPinInput.value.trim();
    if (remote.pin) {
      sessionStorage.setItem("kcPresenterPin", remote.pin);
    }
    if (!remote.pinRequired) {
      remote.presenterUnlocked = true;
      renderPresenter();
      return true;
    }
    if (!remote.pin) {
      if (!silent) setSyncStatus("Enter presenter PIN");
      renderLockedPresenter();
      return false;
    }

    try {
      const response = await fetch(apiUrl("/presenter-auth"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ pin: remote.pin })
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || "Invalid presenter PIN");
      }
      remote.presenterUnlocked = true;
      sessionStorage.setItem("kcPresenterPin", remote.pin);
      setSyncStatus("Presenter notes unlocked");
      renderPresenter();
      return true;
    } catch (error) {
      remote.presenterUnlocked = false;
      if (!silent) setSyncStatus(error.message);
      renderLockedPresenter();
      return false;
    }
  }

  function schedulePush() {
    if (!remote.enabled || remote.role !== "remote" || !hasPresenterAccess()) {
      return;
    }
    clearTimeout(remote.pushTimer);
    remote.pushTimer = setTimeout(pushRemoteState, 120);
  }

  async function pushRemoteState() {
    if (!remote.enabled || !hasPresenterAccess()) {
      return;
    }
    try {
      const response = await fetch(apiUrl("/state"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          session: remote.session,
          pin: remote.pin,
          state: {
            view: state.lastPublicView,
            selectedStoryId: state.selectedStoryId,
            selectedTermId: state.selectedTermId,
            selectedStepId: state.selectedStepId,
            category: state.category,
            query: state.query
          }
        })
      });
      const payload = await response.json();
      if (!response.ok || payload.error) {
        throw new Error(payload.error || "Remote push failed");
      }
      remote.revision = payload.revision;
      setSyncStatus(`${roleLabel()} | pushed r${remote.revision}`);
    } catch (error) {
      setSyncStatus(error.message || "Remote offline");
    }
  }

  async function pollRemoteState() {
    if (!remote.enabled || remote.role !== "screen") {
      return;
    }
    try {
      const response = await fetch(apiUrl(`/state?session=${encodeURIComponent(remote.session)}`), { cache: "no-store" });
      const payload = await response.json();
      if (payload.revision > remote.revision) {
        remote.revision = payload.revision;
        applyRemoteState(payload.state || {});
        setSyncStatus(`${roleLabel()} | synced r${remote.revision}`);
      }
    } catch (_) {
      setSyncStatus("Screen sync offline");
    }
  }

  function applyRemoteState(nextState) {
    if (nextState.view && isPublicView(nextState.view)) {
      state.view = nextState.view;
      state.lastPublicView = nextState.view;
    }
    if (nextState.selectedStoryId) state.selectedStoryId = nextState.selectedStoryId;
    if (nextState.selectedTermId) state.selectedTermId = nextState.selectedTermId;
    if (nextState.selectedStepId) state.selectedStepId = nextState.selectedStepId;
    if (nextState.category) state.category = nextState.category;
    if (typeof nextState.query === "string") state.query = nextState.query;
    els.searchInput.value = state.query;
    renderAll();
  }

  function normalizeSession(value) {
    const normalized = String(value || "demo").trim().replace(/[^a-zA-Z0-9_-]/g, "-");
    return normalized || "demo";
  }

  function roleLabel() {
    return `${remote.role === "remote" ? "Presenter" : "Screen"}: ${remote.session}`;
  }

  function setSyncStatus(message) {
    els.syncStatus.textContent = message;
  }

  function registerServiceWorker() {
    if ("serviceWorker" in navigator) {
      navigator.serviceWorker.register("sw.js").catch(() => undefined);
    }
  }

  async function installPwa() {
    if (!remote.installPrompt) {
      els.installButton.textContent = "Use browser menu";
      setTimeout(() => {
        els.installButton.textContent = "Install";
      }, 1800);
      return;
    }
    remote.installPrompt.prompt();
    await remote.installPrompt.userChoice;
    remote.installPrompt = null;
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function escapeAttr(value) {
    return escapeHtml(value).replace(/`/g, "&#096;");
  }

  init();
})();