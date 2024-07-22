import { buildResults, buildSearch } from "./builds.js";
import {
 clearInputField,
 getSearchTerm,
 removeChildren,
} from "./data.js";
import { fetchSearchData, fetchSuggestions } from "../data/searchData.js";
import { autocomplete } from "../data/suggestions.js";

document.addEventListener("readystatechange", (event) => {
 if (event.target.readyState === "complete") {
  init();
 }
});

let currentPage = 1;

const init = async () => {
 const input = document.getElementById("searchInput");
 input.focus();
 autocomplete(document.getElementById("searchInput"), []);
};

document.getElementById("submitButton").addEventListener("click", async (event) => {
    event.preventDefault();
    const searchTerm = getSearchTerm();
    removeChildren();

    // Update the URL with the search term
    const newUrl = `${window.location.pathname}?query=${encodeURIComponent(searchTerm)}`;
    window.history.pushState({ path: newUrl }, "", newUrl);

    // API: Fetch search results
    const result = await fetchSearchData(searchTerm, 1);
    const resultArray = buildResults(result);
    buildSearch(resultArray);
});

document.getElementById("clearText").addEventListener("click", () => {
 clearInputField();
});

document
    .getElementById("searchInput")
    .addEventListener("input", async (event) => {
     const searchTerm = event.target.value;
     const clear = document.getElementById("clearText");
     // API:
     var allSuggestions = await fetchSuggestions();

     if (searchTerm !== "") {
      clear.classList.remove("none");
      let suggestions = allSuggestions.filter(
          (item) =>
              item.substr(0, searchTerm.length).toUpperCase() ===
              searchTerm.toUpperCase()
      );
      console.log("suggestions bug: " + suggestions);
      autocomplete(searchInput, suggestions);
     } else {
      clear.classList.add("none");
     }
    });

export const loadMoreResults = async (searchTerm) => {
 currentPage += 1;
 const data = await fetchSearchData(searchTerm, currentPage);
 const results = buildResults(data);
 buildSearch(results, true);
};

window.addEventListener("scroll", () => {
 const { scrollTop, scrollHeight, clientHeight } = document.documentElement;

 if (scrollTop + clientHeight >= scrollHeight - 5) {
  const searchTerm = getSearchTerm();
  loadMoreResults(searchTerm);
 }
});
