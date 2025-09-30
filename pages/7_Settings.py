import io, json
import pandas as pd
import streamlit as st

st.title("Settings")

st.markdown("## Preferences & display")
st.caption("Manage your dashboard preferences from here.")


st.session_state.setdefault("imperial_on", False) 
st.session_state.setdefault("safe_ed_mode_on", False) 

with st.container(border=True):
    st.subheader("Display Metric or Imperial", divider="gray")
    st.write("Toggle whether you would like the dashboard to display metric (meters, Kilograms) or imperial (foot, stone).")
    imperial_on = st.toggle(
        "Display imperial",
        value=st.session_state.imperial_on
    )
    if imperial_on != st.session_state.imperial_on:
        st.session_state.imperial_on = imperial_on

with st.container(border=True):
    st.subheader("Safe ED mode on", divider="gray")
    st.write("Toggle whether you would like the dashboard to use safer wording and visualisations.")
    safe_ed_mode_on = st.toggle(
        "Safe ED mode",
        value=st.session_state.safe_ed_mode_on
    )
    if safe_ed_mode_on != st.session_state.safe_ed_mode_on:
        st.session_state.safe_ed_mode_on = safe_ed_mode_on



st.session_state.setdefault("profiling_opt_in", False)        
st.session_state.setdefault("processing_paused", False)        #
st.session_state.setdefault("consent_timestamp", None)

st.markdown("## Privacy & data")
st.caption("Manage your GDPR rights below. Exports include all profile & nutrition/activity records linked to your account.")

with st.container(border=True):
    st.subheader("AI suggestions & profiling", divider="gray")
    st.write("Allow personalised tips based on your data (profiling). You can withdraw consent at any time.")
    new_opt_in = st.toggle(
        "Allow personalised AI suggestions",
        value=st.session_state.profiling_opt_in,
        help="If off, we stop profiling for tips. Core analytics still work where legally necessary."
    )
    if new_opt_in != st.session_state.profiling_opt_in:
        st.session_state.profiling_opt_in = new_opt_in
        st.session_state.consent_timestamp = pd.Timestamp.utcnow().isoformat()
        st.success(("Consent recorded " if new_opt_in else "Consent withdrawn ") + f"({st.session_state.consent_timestamp}).")

with st.container(border=True):
    st.subheader("Pause processing", divider="gray")
    st.write("Temporarily stop non-essential processing & new imports while you review your settings.")
    st.session_state.processing_paused = st.toggle(
        "Pause non-essential processing",
        value=st.session_state.processing_paused
    )

with st.container(border=True):
    st.subheader("Edit my details", divider="gray")
    with st.form("edit_profile"):
        name = st.text_input("Name", value=st.session_state.get("user_name", ""))
        email = st.text_input("Email", value=st.session_state.get("user_email", ""))
        submitted = st.form_submit_button("Save changes")
        if submitted:
            # TODO: call your API to update profile
            st.session_state.user_name = name
            st.session_state.user_email = email
            st.success("Details updated.")

with st.container(border=True):
    st.subheader("Download my data", divider="gray")
    st.write("Get a copy of your data in machine-readable formats (JSON/CSV).")
    # Replace these with your real dataframes / dicts
    profile = {"name": st.session_state.get("user_name",""), "email": st.session_state.get("user_email","")}
    nutrition_df = st.session_state.get("nutrition_df", pd.DataFrame(columns=["date","food","energy_kcal"]))
    activity_df = st.session_state.get("activity_df", pd.DataFrame(columns=["date","type","distance_km"]))

    # JSON bundle
    bundle = {
        "profile": profile,
        "nutrition": nutrition_df.to_dict(orient="records"),
        "activity": activity_df.to_dict(orient="records"),
        "exported_at": pd.Timestamp.utcnow().isoformat()
    }
    json_bytes = io.BytesIO(json.dumps(bundle, ensure_ascii=False, indent=2).encode("utf-8"))
    st.download_button("Download JSON export", data=json_bytes, file_name="schemanest_export.json", mime="application/json")

    # CSV examples
    if not nutrition_df.empty:
        st.download_button("Download nutrition.csv", data=nutrition_df.to_csv(index=False).encode("utf-8"),
                           file_name="nutrition.csv", mime="text/csv")
    if not activity_df.empty:
        st.download_button("Download activity.csv", data=activity_df.to_csv(index=False).encode("utf-8"),
                           file_name="activity.csv", mime="text/csv")

with st.container(border=True):
    st.subheader("Delete my account", divider="gray")
    st.write("Permanently delete your account and associated personal data. Some records may be retained where required by law (e.g., audit/tax).")
    if st.button("Request deletion"):
        # TODO: call your API to create a verified deletion ticket
        st.warning("Deletion request submitted. We'll confirm by email and process it as required by law.")

with st.container(border=True):
    st.subheader("Subject Access Request (SAR) & contact", divider="gray")
    st.write("You can request copies of your information or raise a concern. We normally respond within one month (extendable if complex).")
    # TODO: link to your helpdesk/email
    st.link_button("Open SAR form", "https://schemanest.com/sar")
    st.caption("Data Protection contact: privacy@schemanest.com")