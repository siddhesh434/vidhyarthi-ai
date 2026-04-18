# app.py
# Vidyarthi-AI — Multi-Chat NCERT Tutor with Persistent History & In-Context Quiz
# Deployed via Databricks Apps

import streamlit as st
import uuid
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# --- Databricks Spark Connection ---
from databricks.connect import DatabricksSession

try:
    spark = DatabricksSession.builder.serverless().getOrCreate()
except Exception as e:
    cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")
    if cluster_id:
        spark = DatabricksSession.builder.clusterId(cluster_id).getOrCreate()
    else:
        st.error(f"Failed to initialize Spark connection. Error: {e}")
        raise e

from src.llm_engine import VidyarthiAgent
from src.async_memory_updater import (
    log_chat_to_db,
    submit_quiz_and_update_memory,
    create_chat_session,
    get_user_sessions,
    get_session_messages,
    update_session_title,
    save_quiz_result,
    get_session_quizzes,
)

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Vidyarthi-AI | Bharat Educator",
    page_icon="🎓",
    layout="wide",
)

# ── Premium CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

/* ── Global ─────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif !important;
}

/* ── Gradient Header ────────────────────────── */
.header-banner {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    padding: 1.4rem 2rem;
    border-radius: 14px;
    margin-bottom: 1.2rem;
    color: white;
    box-shadow: 0 8px 32px rgba(102, 126, 234, 0.25);
}
.header-banner h1 { margin: 0; font-size: 1.7rem; font-weight: 700; }
.header-banner p  { margin: 0.25rem 0 0; opacity: 0.85; font-size: 0.92rem; }

/* ── Current Chat Label ─────────────────────── */
.chat-label {
    font-size: 0.85rem;
    color: #aaa;
    padding: 0.2rem 0.8rem;
    margin-bottom: 0.6rem;
}
.chat-label strong { color: #c5b4fc; }

/* ── Empty State ────────────────────────────── */
.empty-state {
    text-align: center;
    padding: 4rem 1rem 3rem;
    color: #777;
}
.empty-state .icon { font-size: 3.5rem; margin-bottom: 0.8rem; }
.empty-state h3   { margin: 0 0 0.4rem; color: #aaa; }
.empty-state p    { font-size: 0.9rem; }

/* ── Score Card ─────────────────────────────── */
.score-card {
    background: linear-gradient(135deg, #1e1e2e, #2d2d3f);
    border-radius: 14px;
    padding: 1.5rem;
    border: 1px solid #3d3d4f;
    text-align: center;
    margin: 1rem 0;
}
.score-card .score {
    font-size: 2.8rem;
    font-weight: 700;
    background: linear-gradient(135deg, #667eea, #764ba2);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.score-card .label { color: #999; font-size: 0.9rem; margin-top: 0.2rem; }

/* ── Sidebar polish ─────────────────────────── */
section[data-testid="stSidebar"] .stButton > button {
    width: 100%;
    border-radius: 8px;
    font-size: 0.85rem;
    transition: all 0.2s ease;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    transform: translateY(-1px);
    box-shadow: 0 4px 16px rgba(102, 126, 234, 0.25);
}

/* ── Divider helper ─────────────────────────── */
.sidebar-heading {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: #888;
    padding: 0.3rem 0;
    margin-top: 0.2rem;
}

/* ── Source Cards ───────────────────────────── */
.source-card {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    background: linear-gradient(135deg, rgba(102,126,234,0.08), rgba(118,75,162,0.08));
    border: 1px solid rgba(102,126,234,0.25);
    border-radius: 10px;
    padding: 0.55rem 0.9rem;
    margin: 0.35rem 0;
    font-size: 0.87rem;
    color: #ccc;
    transition: border-color 0.2s ease;
}
.source-card:hover { border-color: rgba(102,126,234,0.6); }
.source-card .src-icon { font-size: 1.1rem; flex-shrink: 0; }
.source-card .src-meta { flex: 1; }
.source-card .src-meta strong { color: #c5b4fc; }

/* ── PDF Viewer Panel ───────────────────────── */
.pdf-viewer-panel {
    background: linear-gradient(135deg, #1a1a2e, #16213e);
    border: 1px solid rgba(102,126,234,0.35);
    border-radius: 14px;
    padding: 1rem 1.2rem 1.4rem;
    margin-top: 1rem;
    box-shadow: 0 8px 32px rgba(0,0,0,0.4);
}
.pdf-viewer-header {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    margin-bottom: 0.8rem;
    color: #c5b4fc;
    font-weight: 600;
    font-size: 1rem;
}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SESSION STATE — single source of truth
# ══════════════════════════════════════════════════════════════════════════════
_DEFAULTS = {
    "current_session_id": None,   # active chat UUID
    "messages": [],               # in-memory messages for the active chat
    "all_sessions": [],           # cached list of user's sessions
    "current_quiz": None,         # quiz question list (if active)
    "quiz_submitted": False,      # whether current quiz was submitted
    "session_created": False,     # whether the active session is saved to DB
    "needs_refresh": True,        # flag to reload session list
    "pdf_viewer": None,           # dict with pdf_path, page_number, label — None = hidden
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _start_new_chat():
    """Reset state for a brand-new chat session."""
    st.session_state.current_session_id = str(uuid.uuid4())
    st.session_state.messages = []
    st.session_state.current_quiz = None
    st.session_state.quiz_submitted = False
    st.session_state.session_created = False
    st.session_state.needs_refresh = True


def _load_chat(session_id):
    """Switch to an existing chat — pull its messages from Delta Lake."""
    st.session_state.current_session_id = session_id
    st.session_state.messages = get_session_messages(spark, session_id)
    st.session_state.current_quiz = None
    st.session_state.quiz_submitted = False
    st.session_state.session_created = True   # already in DB
    st.session_state.needs_refresh = True


def _refresh_sessions(uid):
    """Fetch user's session list from DB (only when the flag is set)."""
    if st.session_state.needs_refresh and uid:
        try:
            st.session_state.all_sessions = get_user_sessions(spark, uid)
        except Exception:
            st.session_state.all_sessions = []
        st.session_state.needs_refresh = False


def _fmt_time(ts_str):
    """Pretty-print a timestamp string."""
    try:
        dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
        return dt.strftime("%b %d, %I:%M %p")
    except Exception:
        return str(ts_str)[:16]


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    # ── Student Settings ──────────────────────────────────────────────────
    st.markdown("### ⚙️ Student Setup")
    user_id = st.text_input("Student ID", value="Student_IND_001", key="sid_input")
    class_level = st.selectbox("Class", [8, 9, 10], index=0, key="class_input")

    st.markdown("---")

    # ── New Chat Button ───────────────────────────────────────────────────
    if st.button("➕  New Chat", key="new_chat_btn", use_container_width=True, type="primary"):
        _start_new_chat()
        st.rerun()

    st.markdown("---")

    # ── Chat History ──────────────────────────────────────────────────────
    st.markdown('<p class="sidebar-heading">📋 Chat History</p>', unsafe_allow_html=True)

    _refresh_sessions(user_id)

    if not st.session_state.all_sessions:
        st.caption("No chats yet — start your first conversation!")
    else:
        for _i, _sess in enumerate(st.session_state.all_sessions):
            _is_active = _sess["session_id"] == st.session_state.current_session_id
            _icon = "💬" if _is_active else "📄"
            _title = (_sess["title"] or "Untitled Chat")[:38]
            _time = _fmt_time(_sess["updated_at"])
            _label = f"{_icon} {_title}"

            if st.button(
                _label,
                key=f"ses_{_i}_{_sess['session_id'][:8]}",
                use_container_width=True,
                help=f"Last active: {_time}",
            ):
                _load_chat(_sess["session_id"])
                st.rerun()

    st.markdown("---")
    st.markdown("### 🎙️ Speech Support")
    st.caption("Use **Win+H** or **Mac Dictate** in the chat box for native Speech-to-Text!")


# Ensure there's always an active session
if st.session_state.current_session_id is None:
    _start_new_chat()


# ══════════════════════════════════════════════════════════════════════════════
# MAIN AREA
# ══════════════════════════════════════════════════════════════════════════════

# ── Header Banner ─────────────────────────────────────────────────────────────
st.markdown("""
<div class="header-banner">
    <h1>🎓 Vidyarthi-AI</h1>
    <p>The Open NCERT Tutor — Powered by Sarvam AI &amp; Databricks Delta Lake</p>
</div>
""", unsafe_allow_html=True)

# ── Current Chat Title ────────────────────────────────────────────────────────
_current_title = "New Chat"
for _s in st.session_state.all_sessions:
    if _s["session_id"] == st.session_state.current_session_id:
        _current_title = _s["title"] or "New Chat"
        break

st.markdown(
    f'<div class="chat-label">Current chat: <strong>{_current_title}</strong></div>',
    unsafe_allow_html=True,
)

# ── Helper: render source cards under assistant messages ─────────────────────
def _render_sources(sources, msg_index):
    """Shows a collapsible 'Sources' expander with a clickable Open PDF button per source."""
    if not sources:
        return
    with st.expander(f"📚 Sources ({len(sources)} reference{'s' if len(sources) > 1 else ''})", expanded=False):
        for _si, _src in enumerate(sources):
            col_info, col_btn = st.columns([5, 1])
            col_info.markdown(
                f'<div class="source-card">'
                f'<span class="src-icon">📄</span>'
                f'<span class="src-meta"><strong>{_src["label"]}</strong></span>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if col_btn.button(
                "Open PDF",
                key=f"src_open_{msg_index}_{_si}",
                help=f"View {_src['pdf_filename']} at page {_src['page_number']}",
            ):
                # Toggle: close viewer if same source clicked again
                current = st.session_state.pdf_viewer
                if (
                    current
                    and current["pdf_path"] == _src["pdf_path"]
                    and current["page_number"] == _src["page_number"]
                ):
                    st.session_state.pdf_viewer = None
                else:
                    st.session_state.pdf_viewer = _src
                st.rerun()


# ── Chat Messages ─────────────────────────────────────────────────────────────
if not st.session_state.messages:
    st.markdown("""
    <div class="empty-state">
        <div class="icon">💬</div>
        <h3>Start a conversation</h3>
        <p>Ask any question about your NCERT syllabus — in Hindi, English, or Urdu!</p>
    </div>
    """, unsafe_allow_html=True)
else:
    for _mi, _msg in enumerate(st.session_state.messages):
        with st.chat_message(_msg["role"]):
            st.markdown(_msg["content"])
            # Show clickable sources only for assistant messages that have them
            if _msg["role"] == "assistant" and _msg.get("sources"):
                _render_sources(_msg["sources"], _mi)

# ── Chat Input ────────────────────────────────────────────────────────────────
if prompt := st.chat_input("Ask about your syllabus… (Auto-detects language)"):

    # — First message in a brand-new session → create it in DB
    if not st.session_state.session_created:
        _auto_title = prompt[:50] + ("…" if len(prompt) > 50 else "")
        create_chat_session(spark, st.session_state.current_session_id, user_id, _auto_title)
        st.session_state.session_created = True
        st.session_state.needs_refresh = True
    elif len(st.session_state.messages) == 0:
        # Session exists but has no messages yet (edge case) — update title
        _auto_title = prompt[:50] + ("…" if len(prompt) > 50 else "")
        update_session_title(spark, st.session_state.current_session_id, _auto_title)
        st.session_state.needs_refresh = True

    # Show user message
    st.session_state.messages.append({"role": "user", "content": prompt, "sources": []})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Generate AI answer
    with st.chat_message("assistant"):
        with st.spinner("🔍 Searching the NCERT Database…"):
            agent = VidyarthiAgent(spark)
            response, sources = agent.ask_tutor(prompt)   # ← now returns (text, sources)
            log_chat_to_db(
                spark, st.session_state.current_session_id, user_id, prompt, response
            )
        st.markdown(response)
        # Show sources inline right after the answer
        _render_sources(sources, len(st.session_state.messages))

    st.session_state.messages.append({"role": "assistant", "content": response, "sources": sources})
    st.session_state.needs_refresh = True


# ══════════════════════════════════════════════════════════════════════════════
# PDF VIEWER PANEL  (rendered when a source button is clicked)
# ══════════════════════════════════════════════════════════════════════════════
if st.session_state.pdf_viewer:
    _pv = st.session_state.pdf_viewer
    st.markdown(
        f'<div class="pdf-viewer-panel">'
        f'<div class="pdf-viewer-header">'
        f'📖 {_pv["label"]}'
        f'</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    _close_col, _ = st.columns([1, 6])
    if _close_col.button("✕ Close Viewer", key="close_pdf_viewer"):
        st.session_state.pdf_viewer = None
        st.rerun()

    with st.spinner(f"📂 Rendering page {_pv['page_number']} of {_pv['pdf_filename']}…"):
        try:
            import fitz  # PyMuPDF

            # ── 1. Load PDF bytes — prefer local repo file (fast), fall back to Spark ──
            import os as _os
            if _os.path.exists(_pv["pdf_path"]):
                with open(_pv["pdf_path"], "rb") as _f:
                    _pdf_bytes = _f.read()
            else:
                # File not in local workspace — read via Spark (Unity Catalog Volume)
                _pdf_row = (
                    spark.read.format("binaryFile")
                    .load(_pv["pdf_path"])
                    .select("content")
                    .collect()
                )
                if not _pdf_row:
                    st.error(f"⚠️ `{_pv['pdf_filename']}` could not be loaded from the Volume.")
                    st.stop()
                _pdf_bytes = bytes(_pdf_row[0]["content"])

            # ── 2. Render the exact cited page as a high-res PNG image ──────────────
            _doc = fitz.open(stream=_pdf_bytes, filetype="pdf")
            _page_idx = _pv["page_number"] - 1          # fitz is 0-indexed
            _page_idx = max(0, min(_page_idx, len(_doc) - 1))  # clamp to valid range
            _page = _doc.load_page(_page_idx)
            _mat  = fitz.Matrix(2.0, 2.0)               # 2× zoom → crisp 144 DPI render
            _pix  = _page.get_pixmap(matrix=_mat, alpha=False)
            _img_bytes = _pix.tobytes("png")
            _doc.close()

            # ── 3. Display — st.image never hits browser security restrictions ───────
            st.image(
                _img_bytes,
                caption=f"📄 {_pv['label']}",
                use_column_width=True,
            )
            st.caption(
                "💡 Tip: right-click the image → **Open image in new tab** to zoom in."
            )

        except ImportError:
            st.error(
                "⚠️ `PyMuPDF` is not installed. "
                "Add `PyMuPDF` to your `requirements.txt` and redeploy the app."
            )
        except Exception as _e:
            st.error(
                f"⚠️ Could not render `{_pv['pdf_filename']}`.\n\n"
                f"**Path tried:** `{_pv['pdf_path']}`\n\n"
                f"**Error:** `{_e}`"
            )


# ══════════════════════════════════════════════════════════════════════════════
# IN-CONTEXT QUIZ  (expandable section below the chat)
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("---")

with st.expander("📝 Adaptive Quiz — test yourself on this chat's topics", expanded=bool(st.session_state.current_quiz)):

    st.markdown("Generates a 5-question MCQ quiz based on the topics you discussed above.")

    # Generate button
    if st.button("🎯 Generate Quiz Now", key="gen_quiz"):
        topic_msgs = [m["content"] for m in st.session_state.messages if m["role"] == "user"]
        if not topic_msgs:
            st.warning("Ask the tutor at least one question first so we know what to quiz you on!")
        else:
            with st.spinner("🧠 Sarvam AI is crafting your personalized quiz…"):
                topics = ", ".join(topic_msgs[-3:])
                agent = VidyarthiAgent(spark)
                quiz_data = agent.generate_quiz(topics)
                if quiz_data and isinstance(quiz_data, list):
                    st.session_state.current_quiz = quiz_data
                    st.session_state.quiz_submitted = False
                    st.rerun()
                else:
                    st.error("Quiz generation failed — please try again.")

    # Active quiz form
    if st.session_state.current_quiz and not st.session_state.quiz_submitted:
        st.markdown("---")
        with st.form("quiz_form"):
            _user_answers = []
            _correct_answers = []
            _questions_text = []

            for _i, _q in enumerate(st.session_state.current_quiz):
                st.markdown(f"**Q{_i + 1}: {_q['question']}**")
                _choice = st.radio("Select answer:", _q["options"],index=None, key=f"qr_{_i}")
                _user_answers.append(_choice)
                _correct_answers.append(_q["answer"])
                _questions_text.append(_q["question"])
                st.markdown("---")

            _submitted = st.form_submit_button("Submit Quiz for Evaluation")
            if _submitted:
                with st.spinner("Evaluating your responses…"):
                    agent = VidyarthiAgent(spark)
                    score, analysis = submit_quiz_and_update_memory(
                        spark, agent.headers, user_id, class_level,
                        _user_answers, _correct_answers, _questions_text,
                    )
                    # Persist to quiz_history (store analysis in both columns for compatibility)
                    save_quiz_result(
                        spark, st.session_state.current_session_id, user_id,
                        st.session_state.current_quiz, _user_answers, _correct_answers,
                        score, len(st.session_state.current_quiz), analysis, analysis,
                    )
                    st.session_state.quiz_submitted = True
                    st.session_state.last_analysis = analysis
                    st.session_state.last_score = score

                # Score card
                st.markdown(f"""
                <div class="score-card">
                    <div class="score">{score}/{len(st.session_state.current_quiz)}</div>
                    <div class="label">Quiz Complete!</div>
                </div>
                """, unsafe_allow_html=True)
                st.info(f"📋 **Your Analysis:**\n\n{analysis}")
                st.caption("✅ Your Databricks Report Card has been updated!")

    # Past quizzes for this session
    if st.session_state.session_created and st.session_state.current_session_id:
        try:
            _past = get_session_quizzes(spark, st.session_state.current_session_id)
            if _past:
                st.markdown("---")
                st.markdown("**📚 Past Quiz Attempts in This Chat**")
                for _pq in _past:
                    _c1, _c2 = st.columns([1, 5])
                    _c1.metric("Score", f"{_pq['score']}/{_pq['total']}")
                    _c1.caption(_fmt_time(_pq["created_at"]))
                    _c2.info(_pq['strong_point'])  # now stores full analysis paragraph
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# REPORT CARD  (expandable section)
# ══════════════════════════════════════════════════════════════════════════════
with st.expander("📊 My Report Card"):
    st.markdown(f"**Performance Analytics for `{user_id}`** — reads directly from Databricks Delta Lake.")

    if st.button("🌟 Generate Comprehensive Session Report", key="refresh_rc"):
        if not st.session_state.get("current_session_id"):
            st.warning("Please start a chat session and take some quizzes first!")
        else:
            with st.spinner("Synthesizing all your quizzes from this session into a master report..."):
                try:
                    # 1. Fetch all paragraphs for the CURRENT session
                    session_id = st.session_state.current_session_id
                    df = spark.sql(f"""
                        SELECT strong_point as analysis 
                        FROM bharat_bricks_sol.default.quiz_history
                        WHERE session_id = '{session_id}'
                        ORDER BY created_at ASC
                    """).toPandas()

                    if df.empty:
                        st.info("No quizzes taken in this session yet. Take a quiz to generate your profile!")
                    else:
                        paragraphs = df['analysis'].tolist()
                        
                        # 2. Synthesize using Sarvam 105b
                        agent = VidyarthiAgent(spark)
                        synthetic_report = agent.synthesize_evaluations(paragraphs)
                        
                        # 3. Save the synthesized super-profile to the global memory (purging old rows)
                        safe_report = synthetic_report.replace("'", "''")
                        spark.sql(f"DELETE FROM bharat_bricks_sol.default.user_memory WHERE user_id = '{user_id}'")
                        spark.sql(f"""
                            INSERT INTO bharat_bricks_sol.default.user_memory
                            VALUES ('{user_id}', {class_level}, 'Evaluated', '{safe_report}', '{safe_report}')
                        """)
                        
                        # 4. Display
                        st.success("✅ **Session Synthesis Complete!**")
                        st.info(f"📋 **Your Comprehensive Master Report:**\n\n{synthetic_report}")
                        
                except Exception as e:
                    st.error(f"Failed to generate report: {e}")
