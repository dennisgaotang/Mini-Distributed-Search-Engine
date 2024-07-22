import config from "../config.json" assert { type: "json" };

export const fetchSearchData = async (searchTerm, page = 1) => {
  try {
    const response = await fetch(
        `http://${config.server_host}:${config.server_port}/search?query=${searchTerm}`
    );
    const results = await response.json();
    console.log("RESULTS: ", results);

    const itemsPerPage = 10;
    const startIndex = (page - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pages = Object.values(results);
    const pageItems = pages.slice(startIndex, endIndex);

    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({ pageItems });
      }, 500);
    });
  } catch (error) {
    console.error("Error fetching search data:", error);
    throw error;
  }
};

export const fetchSuggestions = async () => {
  let suggestions = [];
  await fetch(`http://${config.server_host}:${config.server_port}/words`)
    .then((res) => (suggestions = res.json()))
    .then((data) => (suggestions = data));

  console.log("SUGGESTIONS: " + suggestions);
  return suggestions;
};
