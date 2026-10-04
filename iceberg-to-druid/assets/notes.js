const navigation = document.querySelector('.chapter-nav');
const mobile = window.matchMedia('(max-width: 900px)');
const syncNavigation = () => { navigation.open = !mobile.matches; };
syncNavigation();
mobile.addEventListener('change', syncNavigation);
for (const button of document.querySelectorAll('.diagram-zoom')) {
  button.hidden = false;
  button.addEventListener('click', () => {
    const enlarged = button.closest('.diagram').classList.toggle('actual-size');
    button.setAttribute('aria-pressed', String(enlarged));
    button.textContent = enlarged ? 'Fit to page' : 'Actual size';
  });
}
