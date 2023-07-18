function createIcon(iconClass) {
    return `
    <svg class="dataset-icon ${iconClass}" viewBox="0 0 40 40">
        <ellipse cx="20" cy="8" rx="18" ry="6"/>
        <path d="M2 8 Q20 12 38 8"/>
        <ellipse cx="20" cy="20" rx="18" ry="6"/>
        <path d="M2 20 Q20 24 38 20"/>
        <ellipse cx="20" cy="32" rx="18" ry="6"/>
        <path d="M2 32 Q20 36 38 32"/>
    </svg>`;
}
