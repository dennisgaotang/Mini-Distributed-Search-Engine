export const clearInputField = () => {
  const input = document.getElementById("searchInput");
  const clear = document.getElementById("clearText");
  clear.classList.add("none");
  return (input.value = "");
};

export const getSearchTerm = () => {
  const input = document.getElementById("searchInput");
  const term = input.value;
  return term;
};

export const removeChildren = () => {
  const itemsContainer = document.getElementById("resultItems");
  while (itemsContainer.firstChild) {
    itemsContainer.removeChild(itemsContainer.lastChild);
  }
};
